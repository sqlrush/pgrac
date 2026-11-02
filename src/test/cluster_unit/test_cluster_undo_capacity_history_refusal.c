/* Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors.
 * Actual LMON reports, physical classifier and original wait/error observation.
 */
#include "postgres.h"
#define CAPACITY_REFUSAL_TEST_GUC_DEFINED
bool cluster_trace_undo_capacity_refusal = true;
#define UNDO_CAPACITY_HISTORY_FIXTURE_MAIN capacity_history_refusal_parent_main
#include "test_cluster_undo_capacity_history.c"
#undef main

static UndoSegmentHeaderData maximum_pool_before[CLUSTER_UNDO_SEGS_PER_INSTANCE];

static void
history_fill_maximum_pool(int configured_cap)
{
	cluster_undo_segments_max_per_instance = configured_cap;
	for (unsigned segment = 16; segment < 256; segment++)
	{
		UndoSegmentHeaderData *header = &history_headers[segment];

		*header = history_headers[15];
		header->segment_id = segment + 1;
		for (unsigned slot = 0; slot < TT_SLOTS_PER_SEGMENT; slot++)
			header->tt_slots[slot].xid = 7001 + segment * TT_SLOTS_PER_SEGMENT + slot;
		history_present[segment] = true;
	}
	memcpy(maximum_pool_before, history_headers, sizeof(history_headers));
}

static const char *
history_refusal_detail(void)
{
#ifdef CLUSTER_UNDO_CAPACITY_DIAGNOSTIC_V1
	return cluster_undo_cleaner_capacity_refusal_detail(1, 100, true);
#else
	return NULL;
#endif
}

UT_TEST(test_real_protective_pair_keeps_pass_and_actual_report_sequences)
{
	for (unsigned duplicate = 0; duplicate < 2; duplicate++) {
		ClusterCtrcTxnKeyV1 key = { 0 };
		const char *detail;
		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		CtrcCapacitySegmentCursor = 0;
		history_schedule = duplicate ? 4 : 2;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &key));
		detail = history_refusal_detail();
		UT_ASSERT_NOT_NULL(detail);
		if (detail != NULL) {
			UT_ASSERT(strstr(detail, "site=protected_history") != NULL);
			UT_ASSERT(strstr(detail, "history_known=1") != NULL);
			UT_ASSERT(strstr(detail, "pass.complete=1") != NULL);
			UT_ASSERT(strstr(detail, "local_report_sequence=2") != NULL);
			UT_ASSERT(strstr(detail, "first_report_sequence=1") != NULL);
			UT_ASSERT(strstr(detail, "reports_advance=1 change_current=1") != NULL);
			UT_ASSERT(strstr(detail, "protector=1") != NULL);
		}
		UT_ASSERT_EQ(history_lmon.report_sequence, 2);
		history_finish(duplicate ? 2 : 1);
	}
}

UT_TEST(test_real_non_history_debt_is_not_labeled_protected_history)
{
	ClusterCtrcTxnKeyV1 key = { 0 };
	const char *detail;
	history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
	history_fill_maximum_pool(256);
	CtrcCapacitySegmentCursor = 0;
	history_headers[255].tt_slots[47].first_undo_block.raw[0] = 1;
	UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &key));
	detail = history_refusal_detail();
	UT_ASSERT_NOT_NULL(detail);
	if (detail != NULL) {
		UT_ASSERT(strstr(detail, "site=history_incomplete") != NULL
				  || strstr(detail, "site=history_not_only") != NULL);
		UT_ASSERT(strstr(detail, "reports_advance=-1 change_current=-1") != NULL);
	}
	history_finish(0);
	cluster_undo_segments_max_per_instance = 16;
}

UT_TEST(test_actual_local_and_required_peer_h_sources_are_kept_at_refusal)
{
	static const int peers[] = { 1, 65 };
	for (unsigned i = 0; i < lengthof(peers); i++) {
		ClusterCtrcTxnKeyV1 key = { 0 };
		const char *detail;
		char expected[256];
		int peer = peers[i];

		history_start(CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		history_admit_peer(peer);
		/* Keep both genuine report sources at the same H; distinct real
		 * publications, not the F sample id, supply their sequences. */
		history_publish(1000, CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT);
		history_fill_maximum_pool(256);
		CtrcCapacitySegmentCursor = 0;
		history_headers[255].tt_slots[47].first_undo_block.raw[0] = 1;
		cluster_trace_undo_capacity_refusal = true;
		UT_ASSERT(!cluster_undo_cleaner_wait_for_capacity(1, &key));
		detail = history_refusal_detail();
		UT_ASSERT_NOT_NULL(detail);
		snprintf(expected, sizeof(expected), "source={node=%d,boot=%d,h=1000,clock=3000,"
				 "kind=2,token=91,report_sequence=1}", peer, 1100 + peer);
		if (detail != NULL) {
			UT_ASSERT(strstr(detail, "h_sources_v1={known=1") != NULL);
			UT_ASSERT(strstr(detail, "source={node=0,") != NULL);
			UT_ASSERT(strstr(detail, expected) != NULL);
			UT_ASSERT(strstr(detail, "kind=2,token=91,report_sequence=3}") != NULL);
			UT_ASSERT(strstr(detail, "matched=2,emitted=2,dropped=0,truncated=0}") != NULL);
		}
		UT_ASSERT_EQ(history_lmon.report_sequence, 3);
		UT_ASSERT_EQ(history_lmon.freeze_slots[peer].view.report.report_sequence, 1);
		history_finish(0);
		cluster_undo_segments_max_per_instance = 16;
		history_peer = -1;
	}
}

UT_TEST(test_h_source_snapshot_survives_borrowed_history_retirement)
{
	ClusterHeapFreezeCohortObservation observation = { 0 };
	const char *detail;

	/* Diagnostic input only: equal-time SCNs may differ in their node bits.
	 * This supplies no capacity verdict, source protection or admission. */
	observation.horizon_scn = 1000;
	observation.admitted_members_lo = UINT64CONST(5);
	observation.admitted_members_hi = UINT64CONST(2);
	observation.admission_fence.admitted_incarnation[0] = 501;
	observation.admission_fence.admitted_incarnation[65] = 565;
	observation.source_local_horizon_scn = 1000;
	observation.source_local_history.sample_clock_scn = 3000;
	observation.source_local_history.contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK;
	observation.source_local_report_sequence = 77;
	observation.source_peer_horizon_scn[65] = (UINT64CONST(7) << 56) | 1000;
	observation.source_peer_history[65].sample_clock_scn = 4000;
	observation.source_peer_history[65].contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_PREPARED;
	observation.source_peer_history[65].contributor_token = 987;
	observation.source_peer_report_sequence[65] = 88;
	/* A nonrequired matching source must not be reported as a contributor. */
	observation.source_peer_horizon_scn[1] = 1000;
	/* A required report above the minimum is not a limiting contributor. */
	observation.source_peer_horizon_scn[2] = 2000;
	MemSet(&undo_capacity_refusal, 0, sizeof(undo_capacity_refusal));
	cluster_trace_undo_capacity_refusal = true;
	undo_capacity_refusal_capture(&(UndoCapacityRefusalInput){
		.site = "fixture", .segment = 1, .history = &observation });
	MemSet(&observation, 0, sizeof(observation));
	detail = history_refusal_detail();
	UT_ASSERT_NOT_NULL(detail);
	if (detail != NULL) {
		UT_ASSERT(strstr(detail, "source={node=0,boot=501,h=1000,clock=3000,kind=1,token=0,report_sequence=77}") != NULL);
		UT_ASSERT(strstr(detail, "source={node=65,boot=565,h=504403158265496552,clock=4000,kind=3,token=987,report_sequence=88}") != NULL);
		UT_ASSERT(strstr(detail, "source={node=1,") == NULL);
		UT_ASSERT(strstr(detail, "source={node=2,") == NULL);
		UT_ASSERT(strstr(detail, "matched=2,emitted=2,dropped=0,truncated=0}") != NULL);
	}
	UT_ASSERT(history_refusal_detail() == NULL);
}

UT_TEST(test_all_tied_sources_and_route_overflow_are_explicitly_bounded)
{
	ClusterHeapFreezeCohortObservation observation = { 0 };
	UndoCapacityTraceRow row = { 0 };
	const char *detail, *counts;
	unsigned matched = 0, emitted = 0, dropped = 0, truncated = 0;

	observation.horizon_scn = observation.source_local_horizon_scn = 1000;
	observation.admitted_members_lo = observation.admitted_members_hi = UINT64_MAX;
	observation.source_local_history.sample_clock_scn = 3000;
	observation.source_local_report_sequence = 77;
	for (unsigned peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		observation.admission_fence.admitted_incarnation[peer] = 500 + peer;
		observation.source_peer_horizon_scn[peer] = 1000;
		observation.source_peer_history[peer].sample_clock_scn = 3000;
		observation.source_peer_history[peer].contributor_kind = CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT;
		observation.source_peer_history[peer].contributor_token = 900 + peer;
		observation.source_peer_report_sequence[peer] = 1000 + peer;
	}
	MemSet(&undo_capacity_refusal, 0, sizeof(undo_capacity_refusal));
	cluster_trace_undo_capacity_refusal = true;
	cluster_undo_cleaner_capacity_trace_reset();
	row.kind = CAP_TRACE_CURRENT;
	row.gate = "fixture";
	memset(&row.key, 255, sizeof(row.key));
	row.known = CAP_TRACE_KEY | CAP_TRACE_OWNER | CAP_TRACE_SLOT | CAP_TRACE_ADMISSION
		| CAP_TRACE_ROOT | CAP_TRACE_GENERATION | CAP_TRACE_GENERATION_KNOWN;
	for (unsigned i = 0; i < 48; i++) {
		row.position = i;
		cluster_undo_cleaner_capacity_trace_row(&row);
	}
	row.kind = CAP_TRACE_POOL;
	for (unsigned i = 0; i < 16; i++)
		cluster_undo_cleaner_capacity_trace_row(&row);
	undo_capacity_refusal_capture(&(UndoCapacityRefusalInput){
		.site = "fixture", .segment = 1, .history = &observation });
	detail = history_refusal_detail();
	UT_ASSERT_NOT_NULL(detail);
	if (detail != NULL) {
		counts = strstr(detail, ",matched=");
		UT_ASSERT_NOT_NULL(counts);
		if (counts != NULL) {
			UT_ASSERT_EQ(sscanf(counts, ",matched=%u,emitted=%u,dropped=%u,truncated=%u}",
				&matched, &emitted, &dropped, &truncated), 4);
			UT_ASSERT_EQ(matched, 128);
			UT_ASSERT(emitted > 0 && dropped > 0);
			UT_ASSERT_EQ(emitted + dropped, 128);
			UT_ASSERT_EQ(truncated, 1);
		}
		UT_ASSERT(strstr(detail, "route_v2={current=48,pool=16,dropped=0") != NULL);
		UT_ASSERT(strstr(detail, " truncated=1") != NULL);
		UT_ASSERT_EQ(strlen(detail), 16383);
	}
	printf("# source_scratch=%zu row=%zu text=%zu\n", sizeof(UndoCapacityRefusalRecord),
		sizeof(row), sizeof(undo_capacity_refusal_text));
}

UT_TEST(test_unknown_history_and_default_off_do_not_invent_sources)
{
	const char *detail;
	MemSet(&undo_capacity_refusal, 0, sizeof(undo_capacity_refusal));
	cluster_trace_undo_capacity_refusal = false;
	undo_capacity_refusal_capture(&(UndoCapacityRefusalInput){ .site = "fixture", .segment = 1 });
	UT_ASSERT(history_refusal_detail() == NULL);
	cluster_trace_undo_capacity_refusal = true;
	undo_capacity_refusal_capture(&(UndoCapacityRefusalInput){ .site = "fixture", .segment = 1 });
	detail = history_refusal_detail();
	UT_ASSERT_NOT_NULL(detail);
	if (detail != NULL) {
		UT_ASSERT(strstr(detail, "h_sources_v1={known=0}") != NULL);
		UT_ASSERT(strstr(detail, "source={node=") == NULL);
	}
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_real_protective_pair_keeps_pass_and_actual_report_sequences);
	UT_RUN(test_real_non_history_debt_is_not_labeled_protected_history);
	UT_RUN(test_actual_local_and_required_peer_h_sources_are_kept_at_refusal);
	UT_RUN(test_h_source_snapshot_survives_borrowed_history_retirement);
	UT_RUN(test_all_tied_sources_and_route_overflow_are_explicitly_bounded);
	UT_RUN(test_unknown_history_and_default_off_do_not_invent_sources);
	UT_DONE();
	return ut_failed_count != 0;
}
