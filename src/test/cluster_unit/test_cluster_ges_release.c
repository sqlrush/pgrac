/*-------------------------------------------------------------------------
 *
 * test_cluster_ges_release.c
 *    Real nonblocking RELEASE consumer with original GES boundary fixtures.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_ges_release.c
 *
 * NOTES
 *    PGRAC-original standalone tests. Product APIs retain their cluster_
 *    namespace; dependency fixtures do not establish SQL or fabric results.
 *
 *-------------------------------------------------------------------------
 */
int ges_fixture_main(int argc, char **argv);
#define main ges_fixture_main
#include "test_cluster_ges.c"
#undef main

static void
release_fixture(ClusterResId *resid, ClusterGrdHolderId *holder)
{
	cooperative_fixture(resid, holder);
	resid->type = LOCKTAG_RELATION;
	stub_serving_ready = true;
	stub_serving_pending = false;
	stub_readiness_lost_on_release = stub_pending_on_release = false;
	stub_exact_release_result = CLUSTER_GRD_ENTRY_OK;
	stub_exact_release_calls = stub_release_and_drain_calls = 0;
	stub_release_and_drain_result = 0;
}

UT_TEST(test_remote_release_waits_for_actual_ack_without_sleep)
{
	ClusterGesReleaseAttempt attempt = { 0 };
	ClusterResId resid;
	ClusterGrdHolderId holder;
	GesRequestPayload first;
	release_fixture(&resid, &holder);
	UT_ASSERT_EQ(cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 9),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT_EQ(stub_exact_release_calls, 0);
	UT_ASSERT(attempt.exchange.sent && attempt.exchange.wait_registered);
	first = stub_backend_request_last;
	UT_ASSERT_EQ(first.opcode, GES_REQ_OPCODE_RELEASE);
	UT_ASSERT_EQ(first.lock_group_procno_plus_one, 9);
	UT_ASSERT_EQ(first.holder_request_id_lo, holder.request_id);
	stub_now += 100000;
	UT_ASSERT_EQ(cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 9),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT_EQ(memcmp(&first, &stub_backend_request_last, sizeof(first)), 0);
	UT_ASSERT_EQ(stub_cooperative_inserts, 1);
	cooperative_reply(GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 9),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
	UT_ASSERT_EQ(stub_exact_release_calls, 1);
	UT_ASSERT(!attempt.exchange.wait_registered);
	UT_ASSERT_EQ(stub_cooperative_cv_calls, 0);
}

UT_TEST(test_release_refusal_missing_reply_and_error_verdict_are_not_terminal)
{
	for (int fault = 0; fault < 4; fault++) {
		ClusterGesReleaseAttempt attempt = { 0 };
		ClusterResId resid;
		ClusterGrdHolderId holder;
		release_fixture(&resid, &holder);
		if (fault == 0)
			stub_reply_wait_insert_enabled = false;
		if (fault == 1)
			stub_cooperative_outbound_full = true;
		UT_ASSERT_EQ(cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 0),
					 CLUSTER_GES_REDECLARE_PENDING);
		if (fault == 2) {
			stub_cooperative_reply_present = false;
			UT_ASSERT_EQ(cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 0),
						 CLUSTER_GES_REDECLARE_PENDING);
		}
		if (fault == 3) {
			cooperative_reply(GES_REPLY_OPCODE_REJECT, GES_REJECT_REASON_SHARD_FROZEN);
			UT_ASSERT_EQ(cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 0),
						 CLUSTER_GES_REDECLARE_REJECTED);
		}
		UT_ASSERT(!attempt.exchange.confirmed);
		UT_ASSERT_EQ(stub_exact_release_calls, 0);
		UT_ASSERT_EQ(stub_cooperative_cv_calls, 0);
	}
}

UT_TEST(test_release_original_route_and_identity_survive_wait)
{
	for (int fault = 0; fault < 5; fault++) {
		ClusterGesReleaseAttempt attempt = { 0 };
		ClusterResId resid;
		ClusterGrdHolderId holder;
		release_fixture(&resid, &holder);
		(void)cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 0);
		cooperative_reply(GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE);
		if (fault == 0)
			stub_current_epoch++;
		if (fault == 1)
			stub_master_generation++;
		if (fault == 2)
			stub_remote_master++;
		if (fault == 3)
			holder.request_id++;
		if (fault == 4)
			stub_cooperative_change_cut_on_poll = true;
		UT_ASSERT(cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 0)
				  != CLUSTER_GES_REDECLARE_CONFIRMED);
		UT_ASSERT_EQ(stub_exact_release_calls, 0);
	}
}

UT_TEST(test_local_release_uses_real_verdict)
{
	for (int rejected = 0; rejected < 2; rejected++) {
		ClusterGesReleaseAttempt attempt = { 0 };
		ClusterResId resid;
		ClusterGrdHolderId holder;
		release_fixture(&resid, &holder);
		stub_remote_master = cluster_node_id;
		stub_release_and_drain_result = rejected ? CLUSTER_GRD_RELEASE_NOT_READY : 0;
		UT_ASSERT_EQ(cluster_ges_release_poll(&attempt, &resid, &holder, 0, 71, 0),
					 rejected ? CLUSTER_GES_REDECLARE_REJECTED : CLUSTER_GES_REDECLARE_CONFIRMED);
		UT_ASSERT_EQ(stub_release_and_drain_calls, 1);
		UT_ASSERT_EQ(stub_cooperative_inserts, 0);
		UT_ASSERT_EQ(stub_backend_request_enqueue_count, 0);
	}
}

UT_TEST(test_registering_reentry_keeps_original_wait_key)
{
	ClusterGesReleaseAttempt attempt = { 0 };
	ClusterResId resid;
	ClusterGrdHolderId holder;
	release_fixture(&resid, &holder);
	(void)cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 0);
	/* Exact state at insert's post-publication ERROR boundary, before send. */
	attempt.exchange.sent = false;
	attempt.exchange.wait_registered = false;
	attempt.registering = true;
	attempt.exchange.next_send_at = 0;
	UT_ASSERT_EQ(cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 0),
				 CLUSTER_GES_REDECLARE_PENDING);
	UT_ASSERT(!attempt.registering && attempt.exchange.wait_registered);
	UT_ASSERT_EQ(stub_cooperative_inserts, 1);
	cooperative_reply(GES_REPLY_OPCODE_GRANT, GES_REJECT_REASON_NONE);
	UT_ASSERT_EQ(cluster_ges_release_poll(&attempt, &resid, &holder, 7, 71, 0),
				 CLUSTER_GES_REDECLARE_CONFIRMED);
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_remote_release_waits_for_actual_ack_without_sleep);
	UT_RUN(test_release_refusal_missing_reply_and_error_verdict_are_not_terminal);
	UT_RUN(test_release_original_route_and_identity_survive_wait);
	UT_RUN(test_local_release_uses_real_verdict);
	UT_RUN(test_registering_reentry_keeps_original_wait_key);
	UT_DONE();
	return ut_failed_count != 0;
}
