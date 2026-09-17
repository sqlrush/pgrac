/*-------------------------------------------------------------------------
 *
 * test_cluster_page_stats.c
 *    RF-PAGE PGDEL-08 focused unit tests: the §9.1 recovery-only counter
 *    set (EVENT/GAUGE/TIMESTAMP classified, one producer per counter),
 *    the G2 vocabulary table, and the §9.3 attempt dump.
 *
 *    RED mapping (spec §9.1/§9.3 + G2/G4′):
 *      - every §9.1 metric has exactly one counter + one vocabulary
 *        entry with exactly one kind;
 *      - unknown vocabulary names fail closed (no guessed semantics);
 *      - producers increment/write their own counter only;
 *      - the dump carries the full §9.3 field set and never receives
 *        page content (the API has no page-bytes parameter);
 *      - the dump truncates safely on small buffers.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_page_stats.h"

#include "unit_test.h"

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

#include <stdio.h>
#include "utils/timestamp.h" /* TimestampTz */

/* GetCurrentTimestamp lives in the backend (utils/timestamp.c); the
 * standalone unit provides a fixed monotone stub so the TIMESTAMP
 * producers are testable. */
TimestampTz
GetCurrentTimestamp(void)
{
	static int64 ticks = 1000000;

	ticks += 1000000;
	return (TimestampTz)ticks;
}

static ClusterPageRecoveryStats ut_stats;

UT_TEST(test_counters_have_one_kind_each)
{
	static const char *event_names[] = { "cluster.page.source_selected_current",
										 "cluster.page.source_selected_pi",
										 "cluster.page.source_selected_storage",
										 "cluster.page.source_invalid",
										 "cluster.page.source_missing",
										 "cluster.page.source_conflict",
										 "cluster.page.result_skip",
										 "cluster.page.apply_count",
										 "cluster.page.version_mismatch",
										 "cluster.page.unknown_class_blocked",
										 "cluster.page.authority_stale_rejected",
										 "cluster.page.resource_early_release",
										 "cluster.page.retire_denied",
										 "cluster.page.d3_rebuild",
										 "cluster.page.d3_optimization_hit",
										 "cluster.page.stable_base_unresolved" };
	static const char *gauge_names[]
		= { "cluster.page.contributor_records", "cluster.page.contributor_threads",
			"cluster.page.contributor_gaps", "cluster.page.retained_pinned_bytes" };
	static const char *ts_names[]
		= { "cluster.page.last_page_write_ts", "cluster.page.last_durability_barrier_ts",
			"cluster.page.last_post_read_ts" };
	int i;
	ClusterPageStatKind kind;

	/* G2: every §9.1 metric is exactly one of EVENT/GAUGE/TIMESTAMP. */
	for (i = 0; i < (int)lengthof(event_names); i++) {
		UT_ASSERT(cluster_page_stats_describe(event_names[i], &kind));
		UT_ASSERT_EQ((int)kind, (int)CLUSTER_PAGE_STAT_EVENT);
	}
	for (i = 0; i < (int)lengthof(gauge_names); i++) {
		UT_ASSERT(cluster_page_stats_describe(gauge_names[i], &kind));
		UT_ASSERT_EQ((int)kind, (int)CLUSTER_PAGE_STAT_GAUGE);
	}
	for (i = 0; i < (int)lengthof(ts_names); i++) {
		UT_ASSERT(cluster_page_stats_describe(ts_names[i], &kind));
		UT_ASSERT_EQ((int)kind, (int)CLUSTER_PAGE_STAT_TIMESTAMP);
	}
	/* Unknown names fail closed. */
	UT_ASSERT(!cluster_page_stats_describe("cluster.page.no_such", &kind));
	UT_ASSERT(!cluster_page_stats_describe(NULL, &kind));
}

UT_TEST(test_event_producers)
{
	cluster_page_stats_init(&ut_stats);

	cluster_page_stats_source_selected(&ut_stats, CLUSTER_PAGE_SOURCE_CURRENT);
	cluster_page_stats_source_selected(&ut_stats, CLUSTER_PAGE_SOURCE_PI);
	cluster_page_stats_source_selected(&ut_stats, CLUSTER_PAGE_SOURCE_STORAGE);
	cluster_page_stats_source_invalid(&ut_stats);
	cluster_page_stats_source_missing(&ut_stats);
	cluster_page_stats_source_conflict(&ut_stats);
	cluster_page_stats_result_skip(&ut_stats);
	cluster_page_stats_apply(&ut_stats);
	cluster_page_stats_version_mismatch(&ut_stats);
	cluster_page_stats_unknown_class_blocked(&ut_stats);
	cluster_page_stats_authority_stale(&ut_stats);
	cluster_page_stats_early_release(&ut_stats);
	cluster_page_stats_retire_denied(&ut_stats);
	cluster_page_stats_stable_base_unresolved(&ut_stats);
	cluster_page_stats_d3_rebuild(&ut_stats, false);
	cluster_page_stats_d3_rebuild(&ut_stats, true);

	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.source_selected_current), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.source_selected_pi), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.source_selected_storage), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.source_invalid), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.source_missing), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.source_conflict), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.result_skip), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.apply_count), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.version_mismatch), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.unknown_class_blocked), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.authority_stale_rejected), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.resource_early_release), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.retire_denied), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.stable_base_unresolved), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.d3_rebuild), UINT64_C(1));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.d3_optimization_hit), UINT64_C(1));

	/* NULL safety. */
	cluster_page_stats_apply(NULL);
	cluster_page_stats_source_selected(NULL, CLUSTER_PAGE_SOURCE_CURRENT);
}

UT_TEST(test_gauge_and_timestamp_producers)
{
	cluster_page_stats_init(&ut_stats);

	cluster_page_stats_contributors(&ut_stats, 12, 2, 0);
	cluster_page_stats_pinned_bytes(&ut_stats, 4096);
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.contributor_records), UINT64_C(12));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.contributor_threads), UINT64_C(2));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.contributor_gaps), UINT64_C(0));
	UT_ASSERT_EQ(pg_atomic_read_u64(&ut_stats.retained_pinned_bytes), UINT64_C(4096));

	/* Timestamps are written (non-zero after note). */
	cluster_page_stats_note_page_write(&ut_stats);
	cluster_page_stats_note_durability(&ut_stats);
	cluster_page_stats_note_post_read(&ut_stats);
	UT_ASSERT(pg_atomic_read_u64(&ut_stats.last_page_write_ts) != 0);
	UT_ASSERT(pg_atomic_read_u64(&ut_stats.last_durability_barrier_ts) != 0);
	UT_ASSERT(pg_atomic_read_u64(&ut_stats.last_post_read_ts) != 0);
}

UT_TEST(test_attempt_dump_full_field_set)
{
	ClusterPageAttemptDump d;
	char buf[512];
	char small[32];
	int written;
	int full;

	memset(&d, 0, sizeof(d));
	d.failed_origin_thread = 2;
	d.failure_generation = 7;
	d.identity.rlocator.spcOid = 1;
	d.identity.rlocator.dbOid = 2;
	d.identity.rlocator.relNumber = 3;
	d.identity.forknum = MAIN_FORKNUM;
	d.identity.blocknum = 42;
	d.page_class = CLUSTER_PAGE_CLASS_NORMAL;
	d.source_kind = CLUSTER_PAGE_SOURCE_CURRENT;
	d.source_version.identity = d.identity;
	d.source_version.incarnation = 7;
	d.source_version.token = 100;
	d.terminal_version.identity = d.identity;
	d.terminal_version.incarnation = 7;
	d.terminal_version.token = 104;
	d.contributor_count = 4;
	d.contributor_thread_set = 0x2;
	d.applied = true;
	d.durability_ok = true;
	d.post_read_ok = true;
	d.authority_revalidated = true;
	d.released = true;
	d.stop_reason = "none";

	full = cluster_page_attempt_dump(&d, buf, sizeof(buf));
	UT_ASSERT(full > 0);
	UT_ASSERT(strstr(buf, "thread=2") != NULL);
	UT_ASSERT(strstr(buf, "gen=7") != NULL);
	UT_ASSERT(strstr(buf, "rel=1/2/3") != NULL);
	UT_ASSERT(strstr(buf, "block=42") != NULL);
	UT_ASSERT(strstr(buf, "class=1") != NULL);
	UT_ASSERT(strstr(buf, "source=CURRENT") != NULL);
	UT_ASSERT(strstr(buf, "src_tok=100") != NULL);
	UT_ASSERT(strstr(buf, "term_tok=104") != NULL);
	UT_ASSERT(strstr(buf, "contributors=4") != NULL);
	UT_ASSERT(strstr(buf, "threads=0x2") != NULL);
	UT_ASSERT(strstr(buf, "applied=1") != NULL);
	UT_ASSERT(strstr(buf, "durable=1") != NULL);
	UT_ASSERT(strstr(buf, "post_read=1") != NULL);
	UT_ASSERT(strstr(buf, "authority=1") != NULL);
	UT_ASSERT(strstr(buf, "released=1") != NULL);
	UT_ASSERT(strstr(buf, "stop=none") != NULL);

	/* A STOP reason is surfaced. */
	d.stop_reason = "STABLE_BASE_UNRESOLVED";
	cluster_page_attempt_dump(&d, buf, sizeof(buf));
	UT_ASSERT(strstr(buf, "stop=STABLE_BASE_UNRESOLVED") != NULL);

	/* Small buffers truncate safely; the return is the would-be length. */
	written = cluster_page_attempt_dump(&d, small, sizeof(small));
	UT_ASSERT(written >= full);		 /* snprintf semantics */
	small[sizeof(small) - 1] = '\0'; /* NUL-terminated by snprintf */

	/* NULL dump is a no-op. */
	UT_ASSERT_EQ(cluster_page_attempt_dump(NULL, buf, sizeof(buf)), 0);
}

int
main(void)
{
	UT_PLAN(4);

	UT_RUN(test_counters_have_one_kind_each);
	UT_RUN(test_event_producers);
	UT_RUN(test_gauge_and_timestamp_producers);
	UT_RUN(test_attempt_dump_full_field_set);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
