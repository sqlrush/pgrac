/*-------------------------------------------------------------------------
 * test_cluster_control_cf_poll.c -- cooperative ordinary CF REQUEST.
 * Real GES/GRD/reply table; transport, formation and clock are boundaries.
 * No REDECLARE is used to acquire the new holder.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#define PGRAC_HW_HANDOFF_EMBEDDED
#define PGRAC_REAL_CONTROL_CENSUS
#include "test_cluster_hw_handoff.c"
#include "storage/ipc.h"

bool cluster_lms_enabled = true;
bool
cluster_lms_is_ready(void)
{
	return true;
}
int
LWLockNewTrancheId(void)
{
	return 501;
}
void
LWLockRegisterTranche(int tranche pg_attribute_unused(), const char *name pg_attribute_unused())
{}
void
before_shmem_exit(pg_on_exit_callback callback pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{
	HW_CHECK(false); /* The stable-owner stage is a separate composition. */
}
bool
cluster_recovery_transport_components_current(void)
{
	return false;
}
bool
cluster_ges_dedup_retire_control_request(uint32 node, uint32 procno, uint64 epoch, uint64 request)
{
	(void)node;
	(void)procno;
	(void)epoch;
	(void)request;
	HW_CHECK(false); /* No RETIRE is dispatched by these acquisition tests. */
	return false;
}
ClusterICSendResult
cluster_ic_send_envelope(uint8 type, int32 destination, const void *payload, uint32 length)
{
	(void)type;
	(void)destination;
	(void)payload;
	(void)length;
	HW_CHECK(false);
	return CLUSTER_IC_SEND_HARD_ERROR;
}

static void
cf_poll_setup(ClusterLockAcquireRequest *req, bool local)
{
	cf_case = true;
	cf_mode = ShareLock;
	fault = HW_NORMAL;
	cooperative_case = true;
	setup_case(req, false);
	cooperative_sleeps = 0;
	if (local) {
		UT_ASSERT_EQ(cluster_grd_cancel_reservation_by_id(&req->resid, &req->holder),
					 CLUSTER_GRD_ENTRY_OK);
		cluster_node_id = 3;
		req->holder.node_id = 3;
		UT_ASSERT_EQ(cluster_grd_try_reserve(&req->resid, &req->holder, req->lockmode, 3, NULL,
											 &req->master_gen_snapshot),
					 CLUSTER_GRD_ENTRY_OK);
	}
}

UT_TEST(cf_poll_yields_until_remote_exact_grant)
{
	ClusterLockAcquireRequest req;
	ClusterGesAcquireAttempt attempt = { 0 };
	GesReplyPayload reply = { 0 };
	ClusterICEnvelope env = { 0 };
	ClusterGesAcquireResult result;

	cf_poll_setup(&req, false);
	if (getenv("PGRAC_PRE2_TEST_BLOCKING_CF") != NULL) {
		uint32 reason = cluster_ges_send_cf_request_and_wait(&req.resid, req.lockmode, &req.holder,
															 req.request_id, req.timeout_ms, 0,
															 &req.hw_grant);
		result = reason == 0 ? CLUSTER_GES_ACQUIRE_GRANTED : CLUSTER_GES_ACQUIRE_REJECTED;
	} else
		result = cluster_ges_cf_request_poll(&attempt, &req.resid, req.lockmode, &req.holder,
											 &req.hw_grant);
	UT_ASSERT_EQ(result, CLUSTER_GES_ACQUIRE_PENDING);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	if (result != CLUSTER_GES_ACQUIRE_PENDING) {
		cooperative_case = cf_case = false;
		return;
	}
	UT_ASSERT_EQ(request_sent, 1);
	UT_ASSERT_EQ(master_request.opcode, GES_REQ_OPCODE_REQUEST);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 1);
	UT_ASSERT(!req.hw_grant.grant_observed);
	/* A real reply-table delivery, not a result-code fixture. Master grant
	 * itself is covered by the local conflict/ordinary handoff tests. */
	reply.opcode = GES_REPLY_OPCODE_GRANT;
	reply.reply_for_opcode = GES_REQ_OPCODE_REQUEST;
	reply.holder_node_id = req.holder.node_id;
	reply.holder_procno = req.holder.procno;
	reply.holder_cluster_epoch_lo = req.holder.cluster_epoch;
	reply.holder_request_id_lo = req.holder.request_id;
	memcpy(reply.resid, &req.resid, sizeof(req.resid));
	env.source_node_id = 3;
	env.epoch = 1;
	cluster_ges_reply_handler(&env, &reply);
	UT_ASSERT_EQ(
		cluster_ges_cf_request_poll(&attempt, &req.resid, req.lockmode, &req.holder, &req.hw_grant),
		CLUSTER_GES_ACQUIRE_GRANTED);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	UT_ASSERT_EQ(cluster_ges_reply_wait_table_active_count(), 0);
	UT_ASSERT(cluster_ges_cf_grant_is_current(&req.hw_grant, &req.resid, &req.holder,
											  req.request_id, req.lockmode));
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	cooperative_case = cf_case = false;
}

UT_TEST(cf_poll_local_conflict_does_not_wait_for_its_own_drain)
{
	ClusterLockAcquireRequest req;
	ClusterGesAcquireAttempt attempt = { 0 };
	ClusterGrdHolderId blocker = grd_lifecycle_holder(2, 23, 203);
	LOCKMODE mode;

	cf_poll_setup(&req, true);
	blocker.cluster_epoch = 1;
	UT_ASSERT_EQ(cluster_grd_entry_rebind_or_insert_holder(&req.resid, &blocker, 2, ExclusiveLock),
				 CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(
		cluster_ges_cf_request_poll(&attempt, &req.resid, req.lockmode, &req.holder, &req.hw_grant),
		CLUSTER_GES_ACQUIRE_PENDING);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	UT_ASSERT(!cluster_grd_holder_mode_by_id(&req.resid, &req.holder, NULL));
	UT_ASSERT_EQ(cluster_ges_release_and_drain_local(&req.resid, &blocker), GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(
		cluster_ges_cf_request_poll(&attempt, &req.resid, req.lockmode, &req.holder, &req.hw_grant),
		CLUSTER_GES_ACQUIRE_GRANTED);
	UT_ASSERT(cluster_grd_holder_mode_by_id(&req.resid, &req.holder, &mode));
	UT_ASSERT_EQ(mode, ShareLock);
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	cooperative_case = cf_case = false;
}

UT_TEST(cf_poll_cut_change_keeps_the_original_attempt)
{
	ClusterLockAcquireRequest req;
	ClusterGesAcquireAttempt attempt = { 0 };
	GesReplyWaitKey key;

	cf_poll_setup(&req, false);
	UT_ASSERT_EQ(
		cluster_ges_cf_request_poll(&attempt, &req.resid, req.lockmode, &req.holder, &req.hw_grant),
		CLUSTER_GES_ACQUIRE_PENDING);
	key = attempt.exchange.key;
	ut_mock_epoch++;
	UT_ASSERT_EQ(
		cluster_ges_cf_request_poll(&attempt, &req.resid, req.lockmode, &req.holder, &req.hw_grant),
		CLUSTER_GES_ACQUIRE_CUT_CHANGED);
	UT_ASSERT(memcmp(&key, &attempt.exchange.key, sizeof(key)) == 0);
	UT_ASSERT(!req.hw_grant.grant_observed);
	UT_ASSERT_EQ(request_sent, 1);
	UT_ASSERT_EQ(cooperative_sleeps, 0);
	cluster_ges_reply_wait_delete(&key);
	cooperative_case = cf_case = false;
}

UT_TEST(cf_request_common_barrier_blocks_every_entry_then_admits_control_only)
{
	ClusterLockAcquireRequest req;
	ClusterGesAcquireAttempt attempt = { 0 };
	ClusterGrdShared *shared;
	bool found;

	cf_poll_setup(&req, false);
	setup_recovery_control_fixture(false);
	cluster_control_request_shmem_init();
	ProcGlobal = retained_grd_proc_global;
	cluster_shared_config = true;
	shared = retained_grd_shmem("pgrac cluster grd", sizeof(*shared), &found);
	UT_ASSERT(found);
	pg_atomic_write_u32(&shared->master[cluster_grd_shard_for_resource(&req.resid)], 0);
	cluster_grd_shard_set_phase(cluster_grd_shard_for_resource(&req.resid), GRD_SHARD_REBUILDING);
	req.holder.node_id = 0;
	req.holder.cluster_epoch = 9;
	memset(&req.hw_grant, 0, sizeof(req.hw_grant));
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry(&req), CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING);
	UT_ASSERT_EQ(cluster_lock_acquire_s3_partition_reservation(&req),
				 CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING);
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req),
				 CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING);
	UT_ASSERT_EQ(
		cluster_ges_cf_request_poll(&attempt, &req.resid, req.lockmode, &req.holder, &req.hw_grant),
		CLUSTER_GES_ACQUIRE_PENDING);
	UT_ASSERT(!attempt.exchange.initialized);
	cluster_grd_recovery_lmon_tick();
	UT_ASSERT_EQ(
		cluster_ges_cf_request_poll(&attempt, &req.resid, req.lockmode, &req.holder, &req.hw_grant),
		CLUSTER_GES_ACQUIRE_PENDING);
	cluster_grd_recovery_mark_peer_done(
		2, 9, cluster_grd_dead_bitmap_hash(ut_mock_last_event.dead_bitmap));
	UT_ASSERT_EQ(cluster_lock_acquire_s1_entry(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	/* Stable-owner registration is separately composed by the CF tests.
	 * This leg exercises real GRD/GES permission and real retained S5. */
	UT_ASSERT_EQ(cluster_grd_try_reserve(&req.resid, &req.holder, req.lockmode, 0, NULL,
										 &req.master_gen_snapshot),
				 CLUSTER_GRD_ENTRY_OK);
	UT_ASSERT_EQ(
		cluster_ges_cf_request_poll(&attempt, &req.resid, req.lockmode, &req.holder, &req.hw_grant),
		CLUSTER_GES_ACQUIRE_GRANTED);
	UT_ASSERT_EQ(cluster_lock_acquire_s5_promote(&req), CLUSTER_LOCK_ACQUIRE_OK_GRANTED);
	UT_ASSERT(cluster_grd_holder_mode_by_id(&req.resid, &req.holder, NULL));
	UT_ASSERT_EQ(cluster_grd_shard_phase(cluster_grd_shard_for_resource(&req.resid)),
				 GRD_SHARD_REBUILDING);
	UT_ASSERT(cluster_grd_recovery_in_progress());
	UT_ASSERT_EQ(cluster_grd_retire_request_and_drain(&req.resid, &req.holder, 0, NoLock, NULL, 0),
				 0);
	cluster_shared_config = false;
	cooperative_case = cf_case = false;
	finish_recovery_control_fixture();
}

UT_TEST(initial_shared_formation_uses_real_empty_control_census)
{
	cluster_control_request_shmem_init();
	ProcGlobal = retained_grd_proc_global;
	cluster_shared_config = true;
	/* This existing actual GRD barrier test used to run only non-shared,
	 * omitting the registry that blocked the first shared startup. */
	test_recovery_authority_initial_epoch_zero_is_valid();
	cluster_shared_config = false;
}

int
main(void)
{
	MyBackendType = B_LMON;
	UT_PLAN(5);
	UT_RUN(cf_poll_yields_until_remote_exact_grant);
	UT_RUN(cf_poll_local_conflict_does_not_wait_for_its_own_drain);
	UT_RUN(cf_poll_cut_change_keeps_the_original_attempt);
	UT_RUN(cf_request_common_barrier_blocks_every_entry_then_admits_control_only);
	UT_RUN(initial_shared_formation_uses_real_empty_control_census);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
