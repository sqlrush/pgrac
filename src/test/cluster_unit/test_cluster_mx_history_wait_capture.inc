/* Real collector/graph/native Snapshot lifetime, with external wait events. */
static unsigned history_wait_events, history_capacity_events, history_capacity_left;
static unsigned history_wait_fault;
static ClusterMxHistoryStep
history_collect_event(const ClusterMxHistoryRequestWire *wire)
{
	UT_ASSERT(!content_locked && CritSectionCount == 0 && pinned);
	UT_ASSERT_EQ(remembered, 2);
	history_wait_events++;
	if (wire == NULL)
		history_capacity_events++;
	if (history_wait_fault == 1)
		pg_re_throw();
	if (history_wait_fault == 2)
		return CMXH_REQUALIFY;
	if (history_wait_fault == 3) {
		InterruptPending = true;
		return CMXH_PENDING;
	}
	if (history_wait_fault == 4)
		pg_atomic_write_u32(&history_gate.transition_closed, 1);
	finish_pending = false;
	return CMXH_PENDING;
}
static void
history_collect_wait_sources(void)
{
	history_wait_events = history_capacity_events = history_capacity_left = history_wait_fault = 0;
	wait_input = history_collect_event;
}
static ClusterMxHistoryStep
history_collect_capacity(const ClusterMxHistoryRequest *edge, ClusterMxHistoryRequestWire *out)
{
	if (history_capacity_left) {
		history_capacity_left--;
		return CMXH_PENDING;
	}
	return finish_prepare(edge, out);
}
UT_TEST(history_wait_complete_retains_snapshot_until_consume)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	finish_sources();
	history_collect_wait_sources();
	finish_pending = true;
	UT_ASSERT(begin(s, &c));
	content_locked = false;
	UT_ASSERT_EQ(cluster_mx_history_wait(c), CMXH_COMPLETE);
	UT_ASSERT_EQ(history_wait_events, 1);
	UT_ASSERT_EQ(history_capacity_events, 0);
	UT_ASSERT_EQ(finish_reads, 2);
	UT_ASSERT_EQ(finish_requests, 2);
	UT_ASSERT_EQ(remembered, 2);
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_COMPLETE);
	UT_ASSERT_EQ(remembered, 2);
	content_locked = false;
	UT_ASSERT_EQ(cluster_mx_history_wait(c), CMXH_UNKNOWN);
	UT_ASSERT_EQ(remembered, 2); /* delivered results still retain their lease */
	clean(c, s);
}
UT_TEST(history_wait_capacity_then_same_batch)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	finish_sources();
	history_collect_wait_sources();
	history_capacity_left = 2;
	prepare_input = history_collect_capacity;
	UT_ASSERT(begin(s, &c));
	content_locked = false;
	UT_ASSERT_EQ(cluster_mx_history_wait(c), CMXH_COMPLETE);
	UT_ASSERT_EQ(history_wait_events, 2);
	UT_ASSERT_EQ(history_capacity_events, 2);
	UT_ASSERT_EQ(finish_reads, 2);
	UT_ASSERT_EQ(finish_requests, 2);
	UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_COMPLETE);
	clean(c, s);
}
UT_TEST(history_wait_error_cancels_before_snapshot_release)
{
	Snapshot s = setup();
	ClusterMxHistoryCollector *c = NULL;
	volatile bool caught = false;
	finish_sources();
	history_collect_wait_sources();
	finish_pending = true;
	history_wait_fault = 1;
	UT_ASSERT(begin(s, &c));
	content_locked = false;
	PG_TRY();
	{
		(void)cluster_mx_history_wait(c);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(finish_cancels, 1); /* cancel leaf asserts Snapshot is still held */
	UT_ASSERT_EQ(remembered, 1);
	clean(c, s);
}
UT_TEST(history_wait_interrupt_cancels_whole_attempt)
{
	for (unsigned ready = 0; ready < 2; ++ready) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		volatile bool caught = false;
		finish_sources();
		history_collect_wait_sources();
		finish_pending = true;
		history_wait_fault = 3;
		UT_ASSERT(begin(s, &c));
		content_locked = false;
		InterruptPending = ready != 0;
		PG_TRY();
		{
			(void)cluster_mx_history_wait(c);
		}
		PG_CATCH();
		{
			caught = true;
		}
		PG_END_TRY();
		UT_ASSERT(caught);
		UT_ASSERT_EQ(history_interrupts, 1);
		UT_ASSERT_EQ(finish_cancels, ready ? 0 : 1);
		UT_ASSERT_EQ(history_wait_events, ready ? 0 : 1);
		UT_ASSERT_EQ(remembered, 1);
		clean(c, s);
	}
}
UT_TEST(history_wait_invalid_heap_never_sleeps)
{
	for (unsigned fault = 0; fault < 3; ++fault) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		finish_sources();
		history_collect_wait_sources();
		UT_ASSERT(begin(s, &c));
		content_locked = fault == 0;
		pinned = fault != 1;
		CritSectionCount = fault == 2;
		UT_ASSERT_EQ(cluster_mx_history_wait(c), CMXH_UNKNOWN);
		UT_ASSERT_EQ(history_wait_events, 0);
		UT_ASSERT_EQ(remembered, 1);
		CritSectionCount = 0;
		clean(c, s);
	}
}
UT_TEST(history_wait_requalification_discards_whole_batch)
{
	for (unsigned fault = 2; fault <= 4; fault += 2) {
		Snapshot s = setup();
		ClusterMxHistoryCollector *c = NULL;
		ClusterMxHistoryStep step;
		finish_sources();
		history_collect_wait_sources();
		finish_pending = true;
		history_wait_fault = fault;
		UT_ASSERT(begin(s, &c));
		content_locked = false;
		step = cluster_mx_history_wait(c);
		UT_ASSERT(step == CMXH_REQUALIFY || step == CMXH_UNKNOWN);
		UT_ASSERT_EQ(finish_cancels, 1);
		UT_ASSERT_EQ(finish_reads, 0);
		UT_ASSERT_EQ(remembered, 1);
		UT_ASSERT_EQ(finish_deliver(c, 2), CMXH_UNKNOWN);
		clean(c, s);
	}
}
UT_TEST(history_wait_requires_and_reuses_independent_B)
{
	ClusterCurrentUpdaterChallenge challenge;
	Snapshot s = updater_remote_setup(&challenge);
	ClusterMxHistoryCollector *c = NULL;
	ClusterCurrentUpdaterProof proof;
	history_collect_wait_sources();
	finish_pending = true;
	UT_ASSERT(updater_begin(s, &challenge, &c));
	content_locked = false;
	UT_ASSERT_EQ(cluster_mx_history_wait(c), CMXH_COMPLETE);
	UT_ASSERT_EQ(updater_fetches, 1);
	UT_ASSERT_EQ(finish_reads, 2);
	UT_ASSERT_EQ(cluster_mx_history_wait(c), CMXH_COMPLETE);
	UT_ASSERT_EQ(updater_fetches, 1);
	UT_ASSERT_EQ(updater_deliver(c, &challenge, &proof), CMXH_COMPLETE);
	UT_ASSERT_EQ(proof.verdict, CUCP_MATCH);
	UT_ASSERT_EQ(remembered, 2);
	clean(c, s);
}
