/* Original dump SRF + real shared stats. Only backend/tuplestore leaves and
 * unrelated categories use the existing diagnostic fixture. No SQL/native
 * execution or proof/visibility qualification is supplied by this test. */
#include "postgres.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "utils/timestamp.h"

int k_range_dump_component_main(void);
uint64 k_range_dump_unused_get(int stat);
void *k_range_dump_unused_shmem(const char *name, Size size, bool *found);
TimestampTz k_range_dump_unused_clock(void);

/* Keep the old fixture and its zero getter unchanged. The linked production
 * observer resolves the real getter, shared-memory leaf and clock below. */
#define main k_range_dump_component_main
#define cluster_multixact_current_stats_get k_range_dump_unused_get
#define ShmemInitStruct k_range_dump_unused_shmem
#define GetCurrentTimestamp k_range_dump_unused_clock
#include "test_cluster_debug.c"
#undef GetCurrentTimestamp
#undef ShmemInitStruct
#undef cluster_multixact_current_stats_get
#undef main

ProcessingMode Mode = NormalProcessing;

static const char *const range_dump_keys[] = {
	"range_scan_started_count", "range_scan_complete_count",
	"range_scan_unknown_count", "range_scan_error_count",
	"range_scan_restart_requested_count", "range_scan_restart_requested_max"
};
static const ClusterCurrentMxStatId range_dump_stats[] = {
	CMX_STAT_RANGE_SCAN_STARTED, CMX_STAT_RANGE_SCAN_COMPLETE,
	CMX_STAT_RANGE_SCAN_UNKNOWN, CMX_STAT_RANGE_SCAN_ERROR,
	CMX_STAT_RANGE_SCAN_RESTART, CMX_STAT_RANGE_SCAN_RESTART_MAX
};

TimestampTz
GetCurrentTimestamp(void)
{
	return INT64CONST(946684800000000);
}

void *
ShmemInitStruct(const char *name, Size size, bool *found)
{
	static union {
		uint64 align;
		unsigned char bytes[4096];
	} segment;

	if (strcmp(name, "pgrac current multixact stats") != 0)
		return k_range_dump_unused_shmem(name, size, found);
	UT_ASSERT(size <= sizeof(segment.bytes));
	if (size > sizeof(segment.bytes))
		abort();
	/* Each explicit test initialization represents a fresh counter region. */
	*found = false;
	return segment.bytes;
}

static void
range_dump_capture(void)
{
	LOCAL_FCINFO(fcinfo, 0);
	ReturnSetInfo rsinfo;

	memset(fcinfo, 0, SizeForFunctionCallInfo(0));
	memset(&rsinfo, 0, sizeof(rsinfo));
	captured_dump_row_count = captured_formatted_value_count = 0;
	fcinfo->resultinfo = (fmNodePtr)&rsinfo;
	(void)cluster_dump_state(fcinfo);
}

static void
range_dump_expect(unsigned field, const char *value)
{
	UT_ASSERT_EQ(captured_dump_count("multixact_current", range_dump_keys[field]), 1);
	UT_ASSERT_STR_EQ(captured_dump_value("multixact_current", range_dump_keys[field]), value);
}

UT_TEST(range_dump_uninitialized_zero_is_only_diagnostics)
{
	range_dump_capture();
	UT_ASSERT_EQ(captured_dump_count("multixact_current", NULL), CMX_STAT_COUNT);
	for (unsigned i = 0; i < lengthof(range_dump_keys); i++)
		range_dump_expect(i, "0");
	UT_ASSERT_EQ(cluster_multixact_current_stats_since(), 0);
}

UT_TEST(range_dump_shared_outcomes_do_not_grant_proof)
{
	cluster_multixact_current_stats_shmem_init();
	for (unsigned i = 0; i < 3; i++)
		cluster_multixact_current_stats_bump(CMX_STAT_RANGE_SCAN_STARTED);
	for (unsigned i = 1; i < 5; i++)
		cluster_multixact_current_stats_bump(range_dump_stats[i]);
	cluster_multixact_current_stats_record_range_restarts(UINT64CONST(4294967302));
	cluster_multixact_current_stats_record_restarts(9);
	cluster_multixact_current_stats_bump(CMX_STAT_DESCRIBE_REMOTE_UNKNOWN);
	range_dump_capture();
	range_dump_expect(0, "3");
	for (unsigned i = 1; i < 5; i++)
		range_dump_expect(i, "1");
	range_dump_expect(5, "4294967302");
	UT_ASSERT_STR_EQ(captured_dump_value("multixact_current", "restart_max"), "9");
	UT_ASSERT_STR_EQ(captured_dump_value("multixact_current", "describe_remote_unknown_count"), "1");
	UT_ASSERT_STR_EQ(captured_dump_value("multixact_current", "member_proof_hit_count"), "0");
	UT_ASSERT_STR_EQ(captured_dump_value("multixact_current", "recompose_success_count"), "0");
}

UT_TEST(range_dump_fields_remain_independent)
{
	for (unsigned selected = 0; selected < lengthof(range_dump_stats); selected++) {
		cluster_multixact_current_stats_shmem_init();
		if (selected == 5)
			cluster_multixact_current_stats_record_range_restarts(7);
		else
			cluster_multixact_current_stats_bump(range_dump_stats[selected]);
		range_dump_capture();
		for (unsigned field = 0; field < lengthof(range_dump_keys); field++)
			range_dump_expect(field, field == selected ? (selected == 5 ? "7" : "1") : "0");
	}
}

UT_TEST(range_dump_uint64_max_is_not_truncated_or_signed)
{
	const uint64 values[] = {UINT64CONST(4294967302), UINT64CONST(9223372036854775808), UINT64_MAX};
	const char *const strings[] = {"4294967302", "9223372036854775808", "18446744073709551615"};

	cluster_multixact_current_stats_shmem_init();
	for (unsigned i = 0; i < lengthof(values); i++) {
		cluster_multixact_current_stats_record_range_restarts(values[i]);
		cluster_multixact_current_stats_record_range_restarts(3);
		range_dump_capture();
		range_dump_expect(5, strings[i]);
		UT_ASSERT_EQ(cluster_multixact_current_stats_get(CMX_STAT_RANGE_SCAN_RESTART_MAX), values[i]);
	}
}

UT_TEST(range_dump_is_read_only_and_preserves_all_rows)
{
	ClusterCurrentMxStatsSnapshot before, after;

	UT_ASSERT(cluster_multixact_current_stats_snapshot(1, 7, &before));
	for (unsigned pass = 0; pass < 2; pass++) {
		range_dump_capture();
		UT_ASSERT_EQ(captured_dump_count("multixact_current", NULL), CMX_STAT_COUNT);
		for (int row = 0; row < captured_dump_row_count; row++) {
			if (strcmp(captured_dump_categories[row], "multixact_current") != 0)
				continue;
			UT_ASSERT_EQ(captured_dump_count("multixact_current", captured_dump_keys[row]), 1);
		}
		range_dump_expect(5, "18446744073709551615");
	}
	UT_ASSERT(cluster_multixact_current_stats_snapshot(1, 7, &after));
	UT_ASSERT(memcmp(&before, &after, sizeof(before)) == 0);
}

int
main(void)
{
	cluster_node_id = 1;
	UT_PLAN(5);
	UT_RUN(range_dump_uninitialized_zero_is_only_diagnostics);
	UT_RUN(range_dump_shared_outcomes_do_not_grant_proof);
	UT_RUN(range_dump_fields_remain_independent);
	UT_RUN(range_dump_uint64_max_is_not_truncated_or_signed);
	UT_RUN(range_dump_is_read_only_and_preserves_all_rows);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
