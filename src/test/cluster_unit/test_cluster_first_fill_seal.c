/*-------------------------------------------------------------------------
 *
 * test_cluster_first_fill_seal.c
 *    Exact holder cancellation, transport ownership and quiet receipt bounds.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_first_fill_seal.c
 *
 * NOTES
 *    This is a pgrac-original file.
 *
 *-------------------------------------------------------------------------
 */
#define FIRST_FILL_SEND_FIXTURE_ONLY
#include "test_cluster_first_fill_send.c"
#undef FIRST_FILL_SEND_FIXTURE_ONLY

static GcsBlockDonePayload
seal_request(const GcsBlockFirstFillForwardPayload *fwd)
{
	ClusterPcmReadGrant service;
	GcsBlockDonePayload done = {0};
	UT_ASSERT(GcsBlockFirstFillForwardPayloadGrant(fwd, cluster_node_id, &service));
	done.tag = service.key.tag; done.request_id = service.key.request_id;
	done.epoch = service.key.epoch; done.sender_node = fwd->forward.master_node;
	done.requester_backend_id = service.key.backend_id; done.transition_id = PCM_TRANS_N_TO_S;
	done.reserved_0[0] = GCS_BLOCK_DONE_PENDING_S_SEAL;
	done.reserved_0[1] = (uint8)service.key.requester_node;
	GcsBlockDonePayloadSetPendingSService(&done, &service);
	return done;
}

UT_TEST(seal_stops_saved_reply_and_retains_each_unadmitted_notification)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	GcsBlockDonePayload done = seal_request(&fwd);
	send_forward(&fwd);
	UT_ASSERT_EQ(send_calls, 1);
	send_notice_admit = false;
	UT_ASSERT(gcs_first_fill_holder_seal(&done, 9));
	UT_ASSERT(gcs_first_fill_holder_parks[0].terminal);
	UT_ASSERT_EQ(gcs_first_fill_holder_parks[0].reply_size, 0);
	for (int tick = 0; tick < 3; tick++) {
		send_tick();
		UT_ASSERT_EQ(send_seal_calls, 0);
		UT_ASSERT_EQ(send_owned(), 1);
	}
	send_notice_admit = true;
	send_tick();
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 1);
	UT_ASSERT_EQ(send_seal_calls, 1);
	send_forward(&fwd); /* Quiet receipt prevents a duplicate producer. */
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(send_calls, 1);
	send_seal_result = CLUSTER_IC_SEND_HARD_ERROR;
	send_tick();
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 1);
	send_seal_result = CLUSTER_IC_SEND_WOULD_BLOCK;
	send_tick();
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 0);
	send_tick();
	UT_ASSERT_EQ(send_seal_calls, 3);
	UT_ASSERT_EQ(send_calls, 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(seal_during_native_output_io_never_resumes_the_cancelled_producer)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	GcsBlockDonePayload done = seal_request(&fwd);
	pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&entry.write_io_generation, 1);
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(send_calls, 0);
	UT_ASSERT(gcs_first_fill_holder_seal(&done, 9));
	UT_ASSERT_EQ(send_owned(), 0);
	pg_atomic_fetch_and_u32(&buf.state, ~BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&entry.write_io_generation, 2);
	send_seal_result = CLUSTER_IC_SEND_DONE;
	send_tick();
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 0);
	UT_ASSERT_EQ(send_calls + send_copies, 0);
	UT_ASSERT_EQ(send_nacks, 1);
	ClusterPcmOwnArray = saved;
}

UT_TEST(seal_receipt_never_borrows_or_cancels_same_tag_successor)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	GcsBlockDonePayload done = seal_request(&fwd);
	UT_ASSERT(gcs_first_fill_holder_seal(&done, 9));
	fwd.forward.request_id++;
	pg_atomic_fetch_or_u32(&buf.state, BM_IO_IN_PROGRESS);
	pg_atomic_write_u64(&entry.write_io_generation, 1);
	send_forward(&fwd);
	UT_ASSERT_EQ(send_owned(), 1);
	send_seal_result = CLUSTER_IC_SEND_DONE;
	send_tick();
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 0);
	UT_ASSERT_EQ(send_owned(), 1);
	UT_ASSERT_EQ(gcs_first_fill_holder_parks[0].forward.forward.request_id, 72);
	UT_ASSERT_EQ(send_nacks + send_calls + send_copies, 0);
	UT_ASSERT(pg_atomic_read_u32(&buf.state) & BM_IO_IN_PROGRESS);
	ClusterPcmOwnArray = saved;
}

UT_TEST(seal_after_image_handoff_waits_for_original_holder_notice_once)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_x_fixture(&buf, &entry);
	GcsBlockDonePayload done = seal_request(&fwd);
	send_result = CLUSTER_IC_SEND_DONE;
	send_forward(&fwd);
	UT_ASSERT_EQ(send_admitted, 1);
	UT_ASSERT(gcs_first_fill_holder_seal(&done, 9));
	send_tick();
	UT_ASSERT_EQ(send_seal_calls, 0);
	UT_ASSERT_EQ(send_owned(), 1);
	send_notice_admit = true; send_seal_result = CLUSTER_IC_SEND_DONE;
	send_stop = true;
	send_tick();
	UT_ASSERT_EQ(send_owned(), 0);
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 0);
	UT_ASSERT_EQ(send_calls, 1);
	UT_ASSERT_EQ(send_holder_handed, 1);
	UT_ASSERT_EQ(send_nacks, 0);
	ClusterPcmOwnArray = saved;
}

UT_TEST(seal_receipt_preserves_unknown_age_and_stop_until_proved_master_end)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	for (int end = 0; end < 5; end++) {
		BufferDesc buf;
		ClusterPcmOwnEntry entry;
		GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
		GcsBlockDonePayload done = seal_request(&fwd);
		const char *reason;
		UT_ASSERT(gcs_first_fill_holder_seal(&done, 9));
		UT_ASSERT_EQ(gcs_first_fill_holder_seal_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
		for (int unknown = 0; unknown < 3; unknown++) {
			send_master = unknown == 0 ? -1 : 2; send_session = unknown != 1;
			send_master_incarnation = unknown == 2 ? 0 : 9;
			send_stop = true; send_now += 1000000000;
			send_tick();
			UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 1);
			UT_ASSERT_EQ(send_seal_calls, 0);
		}
		send_master = 2; send_session = true; send_master_incarnation = 9;
		if (end < 2) send_master_state = end ? CLUSTER_MEMBER_REMOVED : CLUSTER_MEMBER_DEAD;
		else if (end == 2) send_epoch++;
		else if (end == 3) send_master = 3;
		else send_master_incarnation++;
		send_tick();
		UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 0);
		UT_ASSERT_EQ(gcs_first_fill_holder_seal_poll(&reason), CLUSTER_NORMAL_STOP_READY);
	}
	ClusterPcmOwnArray = saved;
}

UT_TEST(seal_receipt_capacity_and_poll_never_evict_a_live_original)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	GcsBlockDonePayload done;
	const char *reason;
	for (int i = 0; i < GCS_FIRST_FILL_PARK_MAX; i++) {
		fwd.forward.request_id = 71 + i; done = seal_request(&fwd);
		UT_ASSERT(gcs_first_fill_holder_seal(&done, 9));
		UT_ASSERT(gcs_first_fill_holder_seal(&done, 9));
		UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, i + 1);
	}
	fwd.forward.request_id++; done = seal_request(&fwd);
	UT_ASSERT(!gcs_first_fill_holder_seal(&done, 9));
	UT_ASSERT_EQ(gcs_first_fill_holder_seals[2][0].forward.forward.request_id, 71);
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_poll(&reason), CLUSTER_NORMAL_STOP_PENDING);
	gcs_first_fill_holder_seal_count--;
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_holder_seal_count++;
	gcs_first_fill_holder_seals[2][0].master_session = 0;
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_poll(&reason), CLUSTER_NORMAL_STOP_INVALID);
	gcs_first_fill_holder_seals[2][0].master_session = 9;
	send_master_incarnation++;
	UT_ASSERT(gcs_first_fill_holder_seal(&done, 10));
	UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 1);
	UT_ASSERT_EQ(gcs_first_fill_holder_seals[2][0].forward.forward.request_id, fwd.forward.request_id);
	ClusterPcmOwnArray = saved;
}

UT_TEST(seal_invalid_identity_cannot_retire_or_replace_original_producer)
{
	ClusterPcmOwnEntry *saved = ClusterPcmOwnArray;
	BufferDesc buf;
	ClusterPcmOwnEntry entry;
	GcsBlockFirstFillForwardPayload fwd = send_setup(&buf, &entry);
	GcsBlockDonePayload original = seal_request(&fwd);
	send_forward(&fwd);
	for (int bad = 0; bad < 7; bad++) {
		GcsBlockDonePayload done = original;
		if (bad == 0) done.reserved_0[15] = 0;
		else if (bad == 1) done.reserved_0[14] = 2;
		else if (bad == 2) done.sender_node = 3;
		else if (bad == 3) done.reserved_0[2] = 0;
		else if (bad == 4) done.requester_backend_id = MaxBackends + 1;
		else if (bad == 5) done.request_id = UINT64_MAX;
		else done.reserved_0[16] = 1;
		UT_ASSERT(!gcs_first_fill_holder_seal(&done, 9));
		UT_ASSERT_EQ(gcs_first_fill_holder_seal_count, 0);
		UT_ASSERT_EQ(send_owned(), 1);
		UT_ASSERT_EQ(send_nacks, 0);
	}
	ClusterPcmOwnArray = saved;
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(seal_stops_saved_reply_and_retains_each_unadmitted_notification);
	UT_RUN(seal_during_native_output_io_never_resumes_the_cancelled_producer);
	UT_RUN(seal_receipt_never_borrows_or_cancels_same_tag_successor);
	UT_RUN(seal_after_image_handoff_waits_for_original_holder_notice_once);
	UT_RUN(seal_receipt_preserves_unknown_age_and_stop_until_proved_master_end);
	UT_RUN(seal_receipt_capacity_and_poll_never_evict_a_live_original);
	UT_RUN(seal_invalid_identity_cannot_retire_or_replace_original_producer);
	UT_DONE();
	return ut_failed_count != 0;
}
