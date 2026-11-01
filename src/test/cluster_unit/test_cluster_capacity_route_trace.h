/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Passive original-path observations, not injected capacity decisions.
 */
UT_TEST(test_route_trace_current_first_gate_and_native_known_bits)
{
#ifdef CLUSTER_UNDO_CAPACITY_ROUTE_TRACE_V2
	static const unsigned faults[] = { 1, 16, 19, 20, 21, 22, 23, 24, 13 };
	static const char *gates[] = { "gc.owner_terminal", "sample.native_active",
		"sample.native_commit_stable", "sample.admission", "sample.recheck",
		"sample.generation_match", "sample.empty_undo", "sample.admission_record",
		"gc.released" };
	for (unsigned i = 0; i < lengthof(faults); i++) {
		unsigned before[5] = { 0 };
		for (unsigned on = 0; on < 2; on++) {
			ClusterCtrcTxnKeyV1 continuation = { 0 };
			UT_ASSERT(seed_current_gc_supply(0, true));
			(void)gc_invalid_input(faults[i]);
			MemSet(&undo_capacity_refusal, 0, sizeof(undo_capacity_refusal));
			cluster_trace_undo_capacity_refusal = on;
			cluster_undo_cleaner_capacity_trace_reset();
			UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 1000, test_cluster_epoch,
				&continuation), CLUSTER_CTRC_CAPACITY_REFUSE);
			if (!on) {
				before[0] = sample_holds; before[1] = sample_releases;
				before[2] = sample_enters; before[3] = sample_native_calls;
				before[4] = gc_snapshot_calls;
				UT_ASSERT_EQ(undo_capacity_refusal.trace.current_count, 0);
			} else {
				const UndoCapacityTraceRow *row = &undo_capacity_refusal.trace.current[0];
				UT_ASSERT_EQ(undo_capacity_refusal.trace.current_count, 48);
				UT_ASSERT_NOT_NULL(row->gate);
				if (row->gate != NULL) UT_ASSERT_STR_EQ(row->gate, gates[i]);
				UT_ASSERT((row->known & CAP_TRACE_KEY) != 0);
				UT_ASSERT_EQ(row->key.xid, gc_keys[0].xid);
				UT_ASSERT_EQ(row->owner_data.owner.status, gc_owners[0].status);
				UT_ASSERT_EQ(sample_holds, before[0]);
				UT_ASSERT_EQ(sample_releases, before[1]);
				UT_ASSERT_EQ(sample_enters, before[2]);
				UT_ASSERT_EQ(sample_native_calls, before[3]);
				UT_ASSERT_EQ(gc_snapshot_calls, before[4]);
				if (faults[i] == 1 || faults[i] == 20 || faults[i] == 24)
					UT_ASSERT_EQ(row->native_known, 0);
				if (faults[i] == 22)
					UT_ASSERT_EQ(row->native_known, 0); /* resident copy not reached */
				if (faults[i] == 19)
					UT_ASSERT_EQ(row->native_known, 63);
			}
		}
	}
	cluster_trace_undo_capacity_refusal = false;
#else
	UT_ASSERT(false); /* Parent has no original-route observation. */
#endif
}

UT_TEST(test_route_trace_current_is_not_overwritten_by_pool)
{
#ifdef CLUSTER_UNDO_CAPACITY_ROUTE_TRACE_V2
	ClusterCtrcTxnKeyV1 continuation = { 0 };
	ClusterCapacityObservationV1 observation;
	UT_ASSERT(seed_current_gc_supply(0, true));
	gc_owners[0].status = CTS_ACTIVE;
	MemSet(&undo_capacity_refusal, 0, sizeof(undo_capacity_refusal));
	cluster_trace_undo_capacity_refusal = true;
	cluster_undo_cleaner_capacity_trace_reset();
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_current(1, 1000, test_cluster_epoch,
		&continuation), CLUSTER_CTRC_CAPACITY_REFUSE);
	(void)reset_pool_sample(3 * TT_SLOTS_PER_SEGMENT);
	sample_active = true;
	UT_ASSERT_EQ(cluster_ctrc_capacity_probe_pool_v1(1, 1000, test_cluster_epoch,
		&observation), CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT_STR_EQ(undo_capacity_refusal.trace.current[0].gate, "gc.owner_terminal");
	UT_ASSERT_EQ(undo_capacity_refusal.trace.pool_count, 1);
	UT_ASSERT_STR_EQ(undo_capacity_refusal.trace.pool[0].gate, "sample.native_active");
	UT_ASSERT_EQ(undo_capacity_refusal.trace.pool[0].key.segment_id, 4);
	cluster_trace_undo_capacity_refusal = false;
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_route_trace_history_first_mismatch_and_existing_counters)
{
#ifdef CLUSTER_UNDO_CAPACITY_ROUTE_TRACE_V2
	UndoCapacityTraceRow row;
	history_fixture();
	history_current.slots[7].status = CTS_ACTIVE;
	MemSet(&row, 0, sizeof(row));
	row.kind = CAP_TRACE_HISTORY;
	UT_ASSERT_EQ(ctrc_capacity_history_current(&history_current, &history_headers[0], 1000,
		&row), CLUSTER_CAPACITY_UNPROVEN_OR_NO_OWNER);
	UT_ASSERT_STR_EQ(row.gate, "history.owner_committed");
	UT_ASSERT_EQ(row.sample_slot, 7);
	UT_ASSERT_EQ(row.owner_data.owner.status, CTS_ACTIVE);
	UT_ASSERT_EQ(row.native_known, 0);
	history_inputs = false;
	for (unsigned on = 0; on < 2; on++) {
		ClusterUndoCapacityChangeFence fence;
		ClusterCapacityHistoryPass pass = { 0 };
		ClusterCapacityObservationV1 observation;
		test_cluster_epoch = 23;
		history_fixture();
		history_current.slots[7].status = CTS_ACTIVE;
		CtrcCapacitySegmentCursor = 0;
		cluster_undo_capacity_fence_init(&fence);
		cluster_undo_capacity_fence_attach(&fence);
		cluster_trace_undo_capacity_refusal = on;
		(void)cluster_ctrc_capacity_probe_history_v1(1, 1000, test_cluster_epoch,
			NULL, &pass, &observation);
		if (on) {
			UT_ASSERT(undo_capacity_refusal.trace.counters_known);
			UT_ASSERT_EQ(undo_capacity_refusal.trace.current_gets, 0);
			UT_ASSERT(undo_capacity_refusal.trace.current_captured);
			UT_ASSERT_EQ(undo_capacity_refusal.trace.fallback.result, true);
			UT_ASSERT_STR_EQ(undo_capacity_refusal.trace.history.gate, "history.owner_committed");
			UT_ASSERT_EQ(undo_capacity_refusal.trace.history.sample_slot, 7);
		}
		cluster_undo_capacity_fence_attach(NULL);
		history_inputs = false;
	}
	test_cluster_epoch = 13;
	cluster_trace_undo_capacity_refusal = true;
	cluster_undo_cleaner_capacity_trace_reset();
	{
		ClusterTTSlotCurrentSnapshot before = { 0 }, after = { 0 };
		after.slots[7].status = CTS_ACTIVE;
		UT_ASSERT(!capacity_trace_final_snapshot(false, &before, &after));
		UT_ASSERT_EQ(undo_capacity_refusal.trace.final_slot, 7);
		UT_ASSERT_STR_EQ(undo_capacity_refusal.trace.final_field, "owner.status");
		UT_ASSERT_EQ(undo_capacity_refusal.trace.final_before, 0);
		UT_ASSERT_EQ(undo_capacity_refusal.trace.final_after, CTS_ACTIVE);
		UT_ASSERT(capacity_trace_final_snapshot(true, &before, &before));
		UT_ASSERT(undo_capacity_refusal.trace.final_field == NULL);
#ifdef CLUSTER_UNDO_CAPACITY_ROUTE_FINAL_READ_V2
		UT_ASSERT(!capacity_trace_final_read(false));
		UT_ASSERT(undo_capacity_refusal.trace.final_read_known);
		UT_ASSERT(!undo_capacity_refusal.trace.final_read_ready);
		UT_ASSERT_STR_EQ(undo_capacity_refusal.trace.final_field, "snapshot_read");
#else
		UT_ASSERT(false); /* An unexecuted memcmp does not identify a failed read. */
#endif
	}
	cluster_trace_undo_capacity_refusal = false;
#else
	UT_ASSERT(false);
#endif
}

UT_TEST(test_route_trace_caps_loss_and_final_refusal_only)
{
#ifdef CLUSTER_UNDO_CAPACITY_ROUTE_TRACE_V2
	UndoCapacityTraceRow row = { 0 };
	const char *detail;
	MemSet(&undo_capacity_refusal, 0, sizeof(undo_capacity_refusal));
	cluster_trace_undo_capacity_refusal = true;
	cluster_undo_cleaner_capacity_trace_reset();
	row.kind = CAP_TRACE_CURRENT;
	row.gate = "first";
	memset(&row.key, 255, sizeof(row.key)); /* Worst-size text, no decision input. */
	row.known = CAP_TRACE_KEY | CAP_TRACE_OWNER | CAP_TRACE_SLOT | CAP_TRACE_ADMISSION
		| CAP_TRACE_ROOT | CAP_TRACE_GENERATION | CAP_TRACE_GENERATION_KNOWN;
	for (unsigned i = 0; i < 50; i++) {
		row.position = i;
		cluster_undo_cleaner_capacity_trace_row(&row);
	}
	row.kind = CAP_TRACE_POOL;
	for (unsigned i = 0; i < 18; i++) {
		row.position = i;
		cluster_undo_cleaner_capacity_trace_row(&row);
	}
	UT_ASSERT_EQ(undo_capacity_refusal.trace.current_count, 48);
	UT_ASSERT_EQ(undo_capacity_refusal.trace.pool_count, 16);
	UT_ASSERT_EQ(undo_capacity_refusal.trace.dropped, 4);
	UT_ASSERT(cluster_undo_cleaner_capacity_refusal_detail(1, 100, true) == NULL);
	undo_capacity_refusal_capture(&(UndoCapacityRefusalInput){ .site = "fixture", .segment = 1 });
	detail = cluster_undo_cleaner_capacity_refusal_detail(1, 100, true);
	UT_ASSERT_NOT_NULL(detail);
	if (detail != NULL) {
		UT_ASSERT(strstr(detail, "route_v2") != NULL);
		UT_ASSERT(strstr(detail, "dropped=4") != NULL);
		UT_ASSERT(strlen(detail) < 16384);
		UT_ASSERT_EQ(strlen(detail), 16383);
		UT_ASSERT(strstr(detail, "truncated=1") != NULL);
	}
	UT_ASSERT(cluster_undo_cleaner_capacity_refusal_detail(1, 100, true) == NULL);
	printf("# route_scratch=%zu row=%zu text=%zu\n", sizeof(UndoCapacityRefusalRecord),
		sizeof(row), sizeof(undo_capacity_refusal_text));
	cluster_trace_undo_capacity_refusal = false;
#else
	UT_ASSERT(false);
#endif
}
