/* Compose real TT shared statistics with the complete debug SRF. */
#include "postgres.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_undo_record_api.h"
#include "storage/lwlock.h"

int tt_counter_debug_fixture_main(void);
#define main tt_counter_debug_fixture_main
#define cluster_tt_durable_locator_stats_snapshot tt_counter_fixture_locator_snapshot
#define cluster_undo_tt_retained_rollover_stats_snapshot tt_counter_fixture_rollover_snapshot
#define cluster_undo_tt_retention_rollover_count tt_counter_fixture_generation
/* Limit suppression to the inherited link fixture's existing warnings. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-prototypes"
#include "test_cluster_debug.c"
#pragma GCC diagnostic pop
#undef cluster_undo_tt_retention_rollover_count
#undef cluster_undo_tt_retained_rollover_stats_snapshot
#undef cluster_tt_durable_locator_stats_snapshot
#undef main

#include "test_cluster_tt_counter_srf.inc"

static ClusterUndoRecordShared tt_counter_record;
static ClusterTTDurableShared tt_counter_durable;
static pg_atomic_uint64 *tt_counter_cells[5][5];
static const char *const tt_counter_families[] = {
	"runtime_active", "runtime_terminal", "mx_current_origin", "mx_projection"
};
static const char *const tt_counter_suffixes[] = {
	"fallback_count", "fallback_result.found", "fallback_result.missing",
	"fallback_result.ambiguous", "fallback_result.scan_unavailable"
};

static void
tt_counter_reset(void)
{
	MemSet(&tt_counter_record, 0, sizeof(tt_counter_record));
	MemSet(&tt_counter_durable, 0, sizeof(tt_counter_durable));
	UndoRecordShared = &tt_counter_record;
	TTDurableShared = &tt_counter_durable;
	cluster_node_id = 3;
	pg_atomic_init_u64(&tt_counter_record.tt_retention_rollover_count, 0);
	pg_atomic_init_u64(&tt_counter_record.tt_retained_rollover_winner_count, 0);
	pg_atomic_init_u64(&tt_counter_record.tt_retained_rollover_lost_race_count, 0);
	tt_counter_cells[0][0] = &tt_counter_durable.locator_fallback_count;
	tt_counter_cells[0][1] = &tt_counter_durable.locator_found_count;
	tt_counter_cells[0][2] = &tt_counter_durable.locator_missing_count;
	tt_counter_cells[0][3] = &tt_counter_durable.locator_ambiguous_count;
	tt_counter_cells[0][4] = &tt_counter_durable.locator_unavailable_count;
	for (unsigned i = 0; i < 4; i++) {
		tt_counter_cells[i + 1][0] = &tt_counter_durable.locator_family_fallback_count[i];
		tt_counter_cells[i + 1][1] = &tt_counter_durable.locator_family_found_count[i];
		tt_counter_cells[i + 1][2] = &tt_counter_durable.locator_family_missing_count[i];
		tt_counter_cells[i + 1][3] = &tt_counter_durable.locator_family_ambiguous_count[i];
		tt_counter_cells[i + 1][4] = &tt_counter_durable.locator_family_unavailable_count[i];
	}
	for (unsigned i = 0; i < 5; i++)
		for (unsigned j = 0; j < 5; j++)
			pg_atomic_init_u64(tt_counter_cells[i][j], 0);
}

static void
tt_counter_expect(const char *key, const char *value)
{
	UT_ASSERT_EQ(captured_dump_count("undo", key), 1);
	if (captured_dump_count("undo", key) == 1)
		UT_ASSERT_STR_EQ(captured_dump_value("undo", key), value);
}

static void
tt_counter_key(char *key, size_t size, unsigned row, unsigned column)
{
	if (row == 0)
		snprintf(key, size, "old_tt_locator_%s", tt_counter_suffixes[column]);
	else
		snprintf(key, size, "old_tt_locator_family.%s.%s", tt_counter_families[row - 1],
				 tt_counter_suffixes[column]);
}

static void
tt_counter_dump(bool rollover, bool locator)
{
	LOCAL_FCINFO(fcinfo, 0);
	ReturnSetInfo rsinfo;
	char key[128];

	MemSet(fcinfo, 0, SizeForFunctionCallInfo(0));
	MemSet(&rsinfo, 0, sizeof(rsinfo));
	captured_dump_row_count = captured_formatted_value_count = 0;
	fcinfo->resultinfo = (fmNodePtr)&rsinfo;
	(void)cluster_dump_state(fcinfo);
	tt_counter_expect("tt_retained_rollover_stats_available", rollover ? "true" : "false");
	tt_counter_expect("old_tt_locator_stats_available", locator ? "true" : "false");
	UT_ASSERT_EQ(captured_dump_count("undo", "tt_retained_rollover_winner_count"), rollover);
	UT_ASSERT_EQ(captured_dump_count("undo", "tt_retained_rollover_lost_race_count"), rollover);
	UT_ASSERT_EQ(captured_dump_count("undo", "old_tt_locator_origin_node_id"), locator);
	for (unsigned i = 0; i < 5; i++)
		for (unsigned j = 0; j < 5; j++) {
			tt_counter_key(key, sizeof(key), i, j);
			UT_ASSERT_EQ(captured_dump_count("undo", key), locator);
		}
}

UT_TEST(test_tt_srf_independent_availability_and_origin_boundaries)
{
	static const int invalid[] = { INT_MIN, -1, 128, 129, INT_MAX };
	ClusterTTDurableLocatorStats out, before;
	uint64 winner, lost;
	char origin[16];

	tt_counter_reset();
	for (unsigned attached = 0; attached < 4; attached++) {
		UndoRecordShared = (attached & 1) ? &tt_counter_record : NULL;
		TTDurableShared = (attached & 2) ? &tt_counter_durable : NULL;
		tt_counter_dump((attached & 1) != 0, (attached & 2) != 0);
		if (attached & 1) {
			tt_counter_expect("tt_retained_rollover_winner_count", "0");
			tt_counter_expect("tt_retained_rollover_lost_race_count", "0");
		}
	}
	for (int node = 0; node < 128; node++) {
		cluster_node_id = node;
		tt_counter_dump(true, true);
		snprintf(origin, sizeof(origin), "%d", node);
		tt_counter_expect("old_tt_locator_origin_node_id", origin);
	}
	MemSet(&before, 0xa5, sizeof(before));
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		cluster_node_id = invalid[i];
		out = before;
		winner = 11;
		lost = 13;
		UT_ASSERT(!cluster_tt_durable_locator_stats_snapshot(&out));
		UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
		UT_ASSERT(!cluster_undo_tt_retained_rollover_stats_snapshot(&winner, &lost));
		UT_ASSERT_EQ(winner, 11);
		UT_ASSERT_EQ(lost, 13);
		tt_counter_dump(false, false);
	}
	cluster_node_id = 3;
	UT_ASSERT(!cluster_tt_durable_locator_stats_snapshot(NULL));
	UT_ASSERT(!cluster_undo_tt_retained_rollover_stats_snapshot(NULL, &lost));
	UT_ASSERT(!cluster_undo_tt_retained_rollover_stats_snapshot(&winner, NULL));
}

UT_TEST(test_tt_srf_retained_unsigned_counts_are_separate_from_generation)
{
	tt_counter_reset();
	pg_atomic_write_u64(&tt_counter_record.tt_retention_rollover_count, 67);
	pg_atomic_write_u64(&tt_counter_record.tt_retained_rollover_winner_count, UINT64_MAX);
	pg_atomic_write_u64(&tt_counter_record.tt_retained_rollover_lost_race_count,
						UINT64_C(9223372036854775809));
	tt_counter_dump(true, true);
	tt_counter_expect("tt_retained_rollover_winner_count", "18446744073709551615");
	tt_counter_expect("tt_retained_rollover_lost_race_count", "9223372036854775809");
	tt_counter_expect("tt_retention_rollover_count", "67");
	UT_ASSERT_EQ(cluster_undo_tt_retention_rollover_count(), 67);
	UT_ASSERT_EQ(pg_atomic_read_u64(&tt_counter_record.tt_retained_rollover_winner_count),
				 UINT64_MAX);
	UT_ASSERT_EQ(pg_atomic_read_u64(&tt_counter_record.tt_retained_rollover_lost_race_count),
				 UINT64_C(9223372036854775809));
}

UT_TEST(test_tt_srf_locator_maps_each_unsigned_aggregate_and_family_cell)
{
	char key[128];

	/* Each literal input must appear in exactly its own public row. */
	for (unsigned selected = 0; selected < 25; selected++) {
		tt_counter_reset();
		pg_atomic_write_u64(tt_counter_cells[selected / 5][selected % 5], UINT64_MAX);
		tt_counter_dump(true, true);
		for (unsigned i = 0; i < 5; i++)
			for (unsigned j = 0; j < 5; j++) {
				tt_counter_key(key, sizeof(key), i, j);
				tt_counter_expect(key, i * 5 + j == selected ? "18446744073709551615" : "0");
			}
	}
}

UT_TEST(test_tt_srf_consumes_real_call_and_result_hooks)
{
	static const ClusterTTDurableLocate results[] = {
		CLUSTER_TT_DURABLE_LOCATE_FOUND, CLUSTER_TT_DURABLE_LOCATE_MISSING,
		CLUSTER_TT_DURABLE_LOCATE_AMBIGUOUS, CLUSTER_TT_DURABLE_LOCATE_SCAN_UNAVAILABLE,
		(ClusterTTDurableLocate)INT_MAX
	};
	char key[128];

	tt_counter_reset();
	for (unsigned i = 0; i < lengthof(results); i++) {
		cluster_tt_durable_count_locator_call();
		cluster_tt_durable_count_locator_result(results[i]);
	}
	tt_counter_dump(true, true);
	tt_counter_expect("old_tt_locator_fallback_count", "5");
	for (unsigned j = 1; j < 5; j++) {
		tt_counter_key(key, sizeof(key), 0, j);
		tt_counter_expect(key, "1");
	}
	for (unsigned i = 1; i < 5; i++)
		for (unsigned j = 0; j < 5; j++) {
			tt_counter_key(key, sizeof(key), i, j);
			tt_counter_expect(key, "0");
		}
	TTDurableShared = NULL;
	cluster_tt_durable_count_locator_call();
	cluster_tt_durable_count_locator_result(CLUSTER_TT_DURABLE_LOCATE_FOUND);
	tt_counter_dump(true, false);
	TTDurableShared = &tt_counter_durable;
	tt_counter_dump(true, true);
	tt_counter_expect("old_tt_locator_fallback_count", "5");
	tt_counter_expect("old_tt_locator_fallback_result.found", "1");
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_tt_srf_independent_availability_and_origin_boundaries);
	UT_RUN(test_tt_srf_retained_unsigned_counts_are_separate_from_generation);
	UT_RUN(test_tt_srf_locator_maps_each_unsigned_aggregate_and_family_cell);
	UT_RUN(test_tt_srf_consumes_real_call_and_result_hooks);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
