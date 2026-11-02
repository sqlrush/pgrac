/* PGRAC: retained native H through the original sender/cohort/watermark.
 * Native sampler and promise accessor are real; the inherited fixture owns
 * clock, shared-memory and process/transport scheduling boundaries.
 * Author: SqlRush <sqlrush@gmail.com> */
int k_inherited_horizon_main(void);
#define main k_inherited_horizon_main
#include "test_cluster_undo_horizon.c"
#undef main

static void
k_native_cohort(ClusterHeapFreezeCohortObservation *saved,
	UndoHorizonV2MaintenanceObservation *observed)
{
	(void)reset_v2_cohort(false);
	native_history_reset();
	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(saved, NULL, observed),
		CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(saved->source_local_horizon_scn, native_history_clock);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, native_history_clock);
}

UT_TEST(k_sender_refuses_numeric_h_without_a_native_promise)
{
	reset_v2_sender();
	native_history_reset();
	undo_horizon_send_v2(1, &test_sender_sample, &test_retention_sample);
	UT_ASSERT_EQ(test_v2_sends, 0);
	UT_ASSERT_EQ(test_horizon_store.report_sequence, 0);
	UT_ASSERT_EQ(native_history_clocks, 0);
	native_history_enabled = false;
}

UT_TEST(k_sender_uses_the_real_native_promise_without_a_new_cut)
{
	ClusterUndoRetentionSample actual;
	unsigned clocks;

	reset_v2_sender();
	native_history_reset();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&actual));
	clocks = native_history_clocks;
	undo_horizon_send_v2(1, &test_sender_sample, &actual);
	UT_ASSERT_EQ(test_v2_sends, 1);
	UT_ASSERT_EQ(test_horizon_store.report_sequence, 1);
	UT_ASSERT_EQ(test_v2_sent.horizon_scn, actual.horizon_scn);
	UT_ASSERT_EQ(native_history_clocks, clocks);
	UT_ASSERT_EQ(native_history_promise_reads, native_history_promise_unlocks);
	UT_ASSERT(!native_history_locked && !native_xid_locked);
	native_history_enabled = false;
}

UT_TEST(k_supplied_history_cannot_replace_the_native_promise)
{
	ClusterHeapFreezeCohortObservation candidate = { 0 }, zero = { 0 };
	UndoHorizonV2MaintenanceObservation observed;

	(void)reset_v2_cohort(false);
	native_history_reset();
	UT_ASSERT_EQ(undo_horizon_v2_cohort_observe(&candidate,
		&test_retention_sample, &observed), CLUSTER_HEAP_FREEZE_COHORT_INVALID);
	UT_ASSERT(memcmp(&candidate, &zero, sizeof(candidate)) == 0);
	UT_ASSERT_EQ(native_history_clocks, 0);
	native_history_enabled = false;
}

UT_TEST(k_saved_cohort_cannot_raise_its_local_h_above_the_promise)
{
	ClusterHeapFreezeCohortObservation saved, forged;
	UndoHorizonV2MaintenanceObservation observed;
	unsigned clocks;

	k_native_cohort(&saved, &observed);
	clocks = native_history_clocks;
	forged = saved;
	forged.source_local_horizon_scn++;
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_current(&forged),
		CLUSTER_HEAP_FREEZE_COHORT_INVALID);
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_current(&saved),
		CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT_EQ(native_history_clocks, clocks);
	native_history_enabled = false;
}

UT_TEST(k_watermark_publisher_preserves_the_old_candidate_on_uncovered_h)
{
	ClusterHeapFreezeCohortObservation saved, forged;
	ClusterHeapFreezeWatermark before;
	UndoHorizonV2MaintenanceObservation observed;
	uint32 sequence;

	k_native_cohort(&saved, &observed);
	UT_ASSERT(undo_horizon_publish_freeze_watermark(&saved, &observed, test_horizon_now));
	before = test_horizon_store.freeze_watermark;
	sequence = pg_atomic_read_u32(&test_horizon_store.freeze_watermark_seq);
	forged = saved;
	forged.source_local_horizon_scn++;
	UT_ASSERT(!undo_horizon_publish_freeze_watermark(&forged, &observed, test_horizon_now));
	UT_ASSERT(memcmp(&before, &test_horizon_store.freeze_watermark, sizeof(before)) == 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&test_horizon_store.freeze_watermark_seq), sequence);
	native_history_enabled = false;
}

UT_TEST(k_watermark_reader_clears_output_instead_of_resampling_uncovered_h)
{
	ClusterHeapFreezeCohortObservation saved;
	ClusterHeapFreezeWatermark candidate, zero = { 0 };
	UndoHorizonV2MaintenanceObservation observed;
	unsigned clocks;

	k_native_cohort(&saved, &observed);
	UT_ASSERT(undo_horizon_publish_freeze_watermark(&saved, &observed, test_horizon_now));
	clocks = native_history_clocks;
	test_horizon_store.freeze_watermark.horizon_scn = native_history_clock + 1;
	memset(&candidate, 0xa5, sizeof(candidate));
	UT_ASSERT(!cluster_heap_freeze_watermark_sample(&candidate));
	UT_ASSERT(memcmp(&candidate, &zero, sizeof(candidate)) == 0);
	UT_ASSERT_EQ(native_history_clocks, clocks);
	native_history_enabled = false;
}

UT_TEST(k_recovery_refuses_original_h_handoffs_without_erasing_the_promise)
{
	ClusterHeapFreezeCohortObservation saved;
	ClusterHeapFreezeWatermark candidate;
	ClusterUndoRetentionSample history;
	UndoHorizonV2MaintenanceObservation observed;
	SCN floor;

	k_native_cohort(&saved, &observed);
	UT_ASSERT(undo_horizon_publish_freeze_watermark(&saved, &observed, test_horizon_now));
	floor = procArray->cluster_read_scn_floor;
	history.horizon_scn = floor;
	history.clock_scn = floor;
	history.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK;
	history.contributor_token = 0;
	native_history_recovery = true;
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_current(&saved), CLUSTER_HEAP_FREEZE_COHORT_INVALID);
	UT_ASSERT(!cluster_heap_freeze_watermark_sample(&candidate));
	undo_horizon_send_v2(1, &test_sender_sample, &history);
	UT_ASSERT_EQ(test_v2_sends, 0);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, floor);
	native_history_recovery = false;
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_current(&saved), CLUSTER_HEAP_FREEZE_COHORT_READY);
	native_history_enabled = false;
}

UT_TEST(k_later_native_cut_keeps_the_original_lower_h_covered)
{
	ClusterHeapFreezeCohortObservation saved;
	ClusterUndoRetentionSample later;
	ClusterHeapFreezeWatermark before, after;
	UndoHorizonV2MaintenanceObservation observed;
	unsigned clocks;

	k_native_cohort(&saved, &observed);
	UT_ASSERT(undo_horizon_publish_freeze_watermark(&saved, &observed, test_horizon_now));
	UT_ASSERT(cluster_heap_freeze_watermark_sample(&before));
	native_history_clock++;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&later));
	UT_ASSERT(later.horizon_scn > saved.source_local_horizon_scn);
	clocks = native_history_clocks;
	UT_ASSERT_EQ(cluster_heap_freeze_cohort_current(&saved), CLUSTER_HEAP_FREEZE_COHORT_READY);
	UT_ASSERT(cluster_heap_freeze_watermark_sample(&after));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
	UT_ASSERT_EQ(native_history_clocks, clocks);
	UT_ASSERT_EQ(native_history_promise_reads, native_history_promise_unlocks);
	UT_ASSERT(!native_history_locked && !native_xid_locked);
	native_history_enabled = false;
}

int
main(void)
{
	UT_PLAN(8);
	UT_RUN(k_sender_refuses_numeric_h_without_a_native_promise);
	UT_RUN(k_sender_uses_the_real_native_promise_without_a_new_cut);
	UT_RUN(k_supplied_history_cannot_replace_the_native_promise);
	UT_RUN(k_saved_cohort_cannot_raise_its_local_h_above_the_promise);
	UT_RUN(k_watermark_publisher_preserves_the_old_candidate_on_uncovered_h);
	UT_RUN(k_watermark_reader_clears_output_instead_of_resampling_uncovered_h);
	UT_RUN(k_recovery_refuses_original_h_handoffs_without_erasing_the_promise);
	UT_RUN(k_later_native_cut_keeps_the_original_lower_h_covered);
	UT_DONE();
	free(procArray);
	return ut_failed_count != 0;
}
