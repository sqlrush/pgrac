/* Real original slot/landing/CV predicates. Only scheduler and transport leaves
 * are controlled; wait never copies a reply or allocates a request identity. */
static ClusterMxHistoryRequestWire wait_event_wire;
static void
history_reply_event(void)
{
	history_deliver(&wait_event_wire, 6);
}
static void
history_capacity_event(void)
{
	cluster_gcs_block_mx_history_cancel(&wait_event_wire);
}
UT_TEST(history_wait_ready_reply_does_not_sleep_or_consume)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult out;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	history_deliver(&w, 5);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_PENDING);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(history_sends, 1);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 1);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &out), CMXH_COMPLETE);
	UT_ASSERT_EQ(out.data[0], 5);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &out), CMXH_UNKNOWN);
}
UT_TEST(history_wait_period_rechecks_without_resend)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult out;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	for (unsigned i = 0; i < 3; ++i) {
		UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_PENDING);
		UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &out), CMXH_PENDING);
		UT_ASSERT(history_zero(&out, sizeof(out)));
	}
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 3);
	UT_ASSERT_EQ(reply_cv_prepare_calls, reply_cv_cancel_calls);
	UT_ASSERT_EQ(history_sends, 1);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 1);
	history_deliver(&w, 7);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &out), CMXH_COMPLETE);
}
UT_TEST(history_wait_reply_event_keeps_exact_attempt)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult out;
	wait_event_wire = w;
	history_sleep_event = history_reply_event;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_PENDING);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 1);
	UT_ASSERT_EQ(reply_cv_prepare_calls, reply_cv_cancel_calls);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &out), CMXH_COMPLETE);
	UT_ASSERT_EQ(out.data[0], 6);
	UT_ASSERT(memcmp(&out.request, &w, sizeof(w)) == 0);
	UT_ASSERT_EQ(history_sends, 1);
}
UT_TEST(history_wait_hello_refresh_and_outbound_backpressure)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult out;
	route_history_stream_generation = 0;
	UT_ASSERT(!cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_PENDING);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 1);
	UT_ASSERT_EQ(history_outbound_waits, 0);
	UT_ASSERT_EQ(history_sends, 0);
	route_history_stream_generation = 101;
	route_seam.enqueue_ok = false;
	UT_ASSERT(!cluster_gcs_block_mx_history_submit(&w));
	history_outbound_wait = history_outbound_ready = true;
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_PENDING);
	UT_ASSERT_EQ(history_outbound_waits, 1);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 1);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 1);
	route_seam.enqueue_ok = true;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	UT_ASSERT(memcmp(&sent_history, &w, sizeof(w)) == 0);
	history_deliver(&w, 4);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &out), CMXH_COMPLETE);
}
UT_TEST(history_wait_original_capacity_without_reservation)
{
	ClusterMxHistoryRequestWire slots[8], next;
	ClusterMxHistoryRequest edge = history_input();
	memset(&edge.forward, 0, sizeof(edge.forward));
	edge.requester_capability_generation = edge.origin_capability_generation = 0;
	history_reset();
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(NULL), CMXH_PENDING);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
	for (unsigned i = 0; i < 8; ++i)
		slots[i] = history_prepare();
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_prepare_routed(&edge, &next), CMXH_PENDING);
	wait_event_wire = slots[0];
	history_sleep_event = history_capacity_event;
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(NULL), CMXH_PENDING);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 1);
	UT_ASSERT_EQ(reply_cv_prepare_calls, reply_cv_cancel_calls);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 7);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_prepare_routed(&edge, &next), CMXH_COMPLETE);
	UT_ASSERT(ClusterR4WireReadU64(next.bytes) > ClusterR4WireReadU64(slots[7].bytes));
	for (unsigned i = 1; i < 8; ++i)
		cluster_gcs_block_mx_history_cancel(&slots[i]);
	cluster_gcs_block_mx_history_cancel(&next);
}
UT_TEST(history_wait_drift_never_delivers_authority)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	ClusterMxHistoryPeerResult out;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	route_history_stream_generation++;
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_REQUALIFY);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &out), CMXH_REQUALIFY);
	UT_ASSERT(history_zero(&out, sizeof(out)));
	w = history_reset_prepare();
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	reply_wait_epoch_drift = true;
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_PENDING);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_REQUALIFY);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_poll(&w, &out), CMXH_REQUALIFY);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 0);
	w = history_reset_prepare();
	history_outbound_wait = true;
	history_outbound_ready = false;
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_REQUALIFY);
	cluster_gcs_block_mx_history_cancel(&w);
}
UT_TEST(history_wait_error_cancels_cv_registration)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare();
	volatile bool caught = false;
	UT_ASSERT(cluster_gcs_block_mx_history_submit(&w));
	reply_cv_timed_sleep_raise = true;
	reply_cv_sleep_raise_after = 1;
	PG_TRY();
	{
		(void)cluster_gcs_block_mx_history_wait_event(&w);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(reply_cv_prepare_calls, 1);
	UT_ASSERT_EQ(reply_cv_cancel_calls, 1);
	UT_ASSERT_EQ(reply_lock_acquire_calls, reply_lock_release_calls);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 1); /* caller owns cancel */
	cluster_gcs_block_mx_history_cancel(&w);
}
UT_TEST(history_wait_missing_or_wrong_request_is_isolated)
{
	ClusterMxHistoryRequestWire w = history_reset_prepare(), bad = w;
	bad.bytes[200] ^= 1;
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&bad), CMXH_UNKNOWN);
	UT_ASSERT_EQ(reply_cv_prepare_calls, 0);
	UT_ASSERT_EQ(cluster_gcs_block_r4_requester_count(), 1);
	CritSectionCount = 1;
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_UNKNOWN);
	CritSectionCount = 0;
	cluster_gcs_block_mx_history_cancel(&w);
	UT_ASSERT_EQ(cluster_gcs_block_mx_history_wait_event(&w), CMXH_UNKNOWN);
	UT_ASSERT_EQ(reply_cv_timed_sleep_calls, 0);
}
