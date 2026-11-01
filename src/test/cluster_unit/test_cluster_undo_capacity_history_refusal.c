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

int
main(void)
{
	UT_PLAN(2);
	UT_RUN(test_real_protective_pair_keeps_pass_and_actual_report_sequences);
	UT_RUN(test_real_non_history_debt_is_not_labeled_protected_history);
	UT_DONE();
	return ut_failed_count != 0;
}
