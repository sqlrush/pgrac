/*-------------------------------------------------------------------------
 *
 * test_cluster_first_fill_holder_ingress.c
 *    Production holder ingress, active producers and exact cancellation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_first_fill_holder_ingress.c
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#define FIRST_FILL_SEND_FIXTURE_ONLY
#include "test_cluster_first_fill_send.c"
#undef FIRST_FILL_SEND_FIXTURE_ONLY

static void
holder_ingress_full(const GcsBlockFirstFillForwardPayload *forward)
{
	/* Unrelated active producers are the controlled capacity boundary. */
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		bool created;
		GcsBlockFirstFillForwardPayload other = *forward;
		other.forward.request_id = 1000 + i;
		UT_ASSERT(gcs_first_fill_holder_reserve(&other, 9, &created) != NULL);
		UT_ASSERT(created);
	}
}

static GcsBlockDonePayload
holder_ingress_seal(const GcsBlockFirstFillForwardPayload *forward)
{
	ClusterPcmReadGrant service;
	GcsBlockDonePayload done = {0};
	UT_ASSERT(GcsBlockFirstFillForwardPayloadGrant(forward, cluster_node_id, &service));
	done.tag = service.key.tag; done.request_id = service.key.request_id;
	done.epoch = service.key.epoch; done.sender_node = forward->forward.master_node;
	done.requester_backend_id = service.key.backend_id; done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_SEAL; done.reserved_0[1] = 1;
	GcsBlockDonePayloadSetPendingSService(&done, &service);
	return done;
}

UT_TEST(full_holder_copies_original_then_real_handler_produces_once_after_capacity)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry), original = fwd;
	holder_ingress_full(&fwd);
	send_forward(&fwd);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 1);
	UT_ASSERT_EQ(send_calls + send_copies + send_deferred + send_nacks, 0);
	memset(&fwd, 0xee, sizeof(fwd)); /* Transport payload storage is already reusable. */
	UT_ASSERT_EQ(memcmp(&gcs_first_fill_holder_ingress_head->forward, &original, sizeof(original)), 0);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_head->envelope.msg_type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
	/* Model actual retirement of the unrelated active producer lot. */
	memset(gcs_first_fill_holder_parks, 0, sizeof(gcs_first_fill_holder_parks));
	send_tick();
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 0);
	UT_ASSERT_EQ(send_copies, 1);
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT(!gcs_first_fill_holder_parks[0].ingress_pending);
	send_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	send_tick(); send_tick();
	UT_ASSERT_EQ(send_admitted, 1);
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(send_copies, 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(following_exact_seal_cancels_queued_producer_without_active_capacity)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	GcsBlockDonePayload done = holder_ingress_seal(&fwd);
	holder_ingress_full(&fwd); send_forward(&fwd);
	UT_ASSERT(gcs_first_fill_holder_seal(&done, 9));
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 0);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_credit[2], 0);
	UT_ASSERT_EQ(send_owned(), 64);
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 1);
	send_seal_result = CLUSTER_IC_SEND_DONE;
	gcs_first_fill_holder_seal_tick(0);
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 0);
	UT_ASSERT_EQ(send_calls + send_copies + send_nacks + send_deferred, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(queued_duplicate_cannot_replace_immutable_bytes_or_consume_more_credit)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry), original = fwd;
	holder_ingress_full(&fwd); send_forward(&fwd); send_forward(&fwd);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 1);
	GcsBlockForwardPayloadSetExpectedPiWatermarkScn(&fwd.forward, 99);
	send_forward(&fwd);
	UT_ASSERT_EQ(send_deferred, 1);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_credit[2], 1);
	UT_ASSERT_EQ(memcmp(&gcs_first_fill_holder_ingress_head->forward, &original, sizeof(original)), 0);
	UT_ASSERT_EQ(send_calls + send_copies + send_nacks, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(queued_forwards_are_bounded_by_original_master_credit_without_eviction)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	holder_ingress_full(&fwd);
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		fwd.forward.request_id = 71 + i; send_forward(&fwd);
	}
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 64);
	fwd.forward.request_id = 900; send_forward(&fwd);
	UT_ASSERT_EQ(send_deferred, 1);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 64);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_head->forward.forward.request_id, 71);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_tail->forward.forward.request_id, 134);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_credit[2], 64);
	UT_ASSERT_EQ(send_calls + send_copies + send_nacks, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(queued_owner_survives_unknown_age_and_drains_exactly_on_stop)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	const char *reason;
	holder_ingress_full(&fwd); send_forward(&fwd);
	for (int unknown = 0; unknown < 3; unknown++) {
		send_master = unknown == 0 ? -1 : 2; send_session = unknown != 1;
		send_master_incarnation = unknown == 2 ? 0 : 9;
		send_now += 1000000000; send_stop = true;
		gcs_first_fill_holder_ingress_tick(0);
		UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 1);
		UT_ASSERT_EQ(gcs_first_fill_holder_ingress_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	}
	send_master = 2; send_session = true; send_master_incarnation = 9;
	memset(gcs_first_fill_holder_parks, 0, sizeof(gcs_first_fill_holder_parks));
	send_tick();
	UT_ASSERT_EQ(send_nacks, 1);
	UT_ASSERT_EQ(send_calls + send_copies, 0);
	UT_ASSERT_EQ(send_owned() + gcs_first_fill_holder_ingress_count, 0);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_poll(&reason), CLUSTER_NORMAL_STOP_READY);
	ClusterPcmOwnArray = saved;
}

UT_TEST(queued_owner_retires_only_on_proved_master_reconfiguration)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int end = 0; end < 5; end++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		holder_ingress_full(&fwd); send_forward(&fwd);
		if (end < 2) send_master_state = end ? CLUSTER_MEMBER_REMOVED : CLUSTER_MEMBER_DEAD;
		else if (end == 2) send_master_incarnation++;
		else if (end == 3) send_epoch++;
		else send_master = 3;
		gcs_first_fill_holder_ingress_tick(0);
		UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 0);
		UT_ASSERT_EQ(gcs_first_fill_holder_ingress_credit[2], 0);
		UT_ASSERT_EQ(send_calls + send_copies + send_nacks, 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(queued_seal_cannot_consume_same_tag_successor_service_or_instance)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	GcsBlockDonePayload done = holder_ingress_seal(&fwd);
	holder_ingress_full(&fwd); send_forward(&fwd);
	fwd.forward.request_id++; send_forward(&fwd);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 2);
	UT_ASSERT(!gcs_first_fill_holder_seal(&done, 10));
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 2);
	UT_ASSERT(gcs_first_fill_holder_seal(&done, 9));
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 1);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_head->forward.forward.request_id, 72);
	UT_ASSERT_EQ(send_calls + send_copies + send_nacks, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(queued_owner_poll_rejects_corrupt_count_credit_chain_and_carrier)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	const char *reason;
	holder_ingress_full(&fwd); send_forward(&fwd);
	gcs_first_fill_holder_ingress_count++;
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_holder_ingress_count--;
	gcs_first_fill_holder_ingress_credit[2]++;
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_holder_ingress_credit[2]--;
	gcs_first_fill_holder_ingress_head->prev = gcs_first_fill_holder_ingress_head;
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_holder_ingress_head->prev = NULL;
	gcs_first_fill_holder_ingress_head->envelope.source_node_id++;
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_holder_ingress_head->envelope.source_node_id--;
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	ClusterPcmOwnArray = saved;
}

UT_TEST(queued_transfer_keeps_original_carrier_across_tick_to_handler_unknown)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	for (int zero_session = 0; zero_session < 2; zero_session++) {
		fwd = send_setup(&buf, &entry);
		holder_ingress_full(&fwd); send_forward(&fwd);
		memset(gcs_first_fill_holder_parks, 0, sizeof(gcs_first_fill_holder_parks));
		if (zero_session) {
			send_auth_unknown_after = 2;
			send_auth_zero_after = true;
		} else send_route_unknown_after = 2;
		send_tick();
		UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 0);
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT(gcs_first_fill_holder_parks[0].ingress_pending);
		UT_ASSERT_EQ(gcs_first_fill_holder_parks[0].ingress_envelope.msg_type, PGRAC_IC_MSG_GCS_BLOCK_FORWARD);
		UT_ASSERT_EQ(gcs_first_fill_holder_parks[0].ingress_envelope.epoch, fwd.forward.epoch);
		UT_ASSERT_EQ(send_calls + send_copies + send_nacks + send_deferred, 0);
		send_route_unknown_after = send_auth_unknown_after = -1;
		send_tick();
		UT_ASSERT_EQ(send_copies, 1);
		UT_ASSERT(!gcs_first_fill_holder_parks[0].ingress_pending);
		send_result = CLUSTER_IC_SEND_DONE;
		send_tick();
		UT_ASSERT_EQ(send_admitted, 1);
		UT_ASSERT_EQ(send_owned(), 0);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(queued_retirement_credit_guard_is_active_without_assertions)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	volatile bool caught = false;
	holder_ingress_full(&fwd); send_forward(&fwd);
	gcs_first_fill_holder_ingress_credit[2] = 0;
	PG_TRY();
	{
		gcs_first_fill_holder_ingress_forget(gcs_first_fill_holder_ingress_head);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 1);
	UT_ASSERT(gcs_first_fill_holder_ingress_head != NULL);
	gcs_first_fill_holder_ingress_credit[2] = 1;
	gcs_first_fill_holder_ingress_forget(gcs_first_fill_holder_ingress_head);
	UT_ASSERT_EQ(gcs_first_fill_holder_ingress_count, 0);
	ClusterPcmOwnArray = saved;
}

int
main(void)
{
	UT_PLAN(10);
	UT_RUN(full_holder_copies_original_then_real_handler_produces_once_after_capacity);
	UT_RUN(following_exact_seal_cancels_queued_producer_without_active_capacity);
	UT_RUN(queued_duplicate_cannot_replace_immutable_bytes_or_consume_more_credit);
	UT_RUN(queued_forwards_are_bounded_by_original_master_credit_without_eviction);
	UT_RUN(queued_owner_survives_unknown_age_and_drains_exactly_on_stop);
	UT_RUN(queued_owner_retires_only_on_proved_master_reconfiguration);
	UT_RUN(queued_seal_cannot_consume_same_tag_successor_service_or_instance);
	UT_RUN(queued_owner_poll_rejects_corrupt_count_credit_chain_and_carrier);
	UT_RUN(queued_transfer_keeps_original_carrier_across_tick_to_handler_unknown);
	UT_RUN(queued_retirement_credit_guard_is_active_without_assertions);
	UT_DONE();
	return ut_failed_count != 0;
}
