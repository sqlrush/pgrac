/* Compose the real cleaner capacity snapshot with the real debug SRF. */
#include "postgres.h"
#include "cluster/cluster_undo_cleaner.h"

int capacity_debug_fixture_main(void);
#define main capacity_debug_fixture_main
#define cluster_undo_cleaner_capacity_stats_snapshot capacity_debug_fixture_snapshot
/* The inherited link fixture has pre-existing prototype warnings. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-prototypes"
#include "test_cluster_debug.c"
#pragma GCC diagnostic pop
#undef cluster_undo_cleaner_capacity_stats_snapshot
#undef main

static UndoCleanerSharedState capacity_shared;
static unsigned capacity_acquires, capacity_releases, capacity_lock_depth;

static bool
capacity_counter_acquire(LWLock *lock, LWLockMode mode)
{
	UT_ASSERT(lock == &capacity_shared.lwlock);
	UT_ASSERT_EQ(mode, LW_SHARED);
	UT_ASSERT_EQ(capacity_lock_depth, 0);
	capacity_lock_depth++;
	capacity_acquires++;
	return true;
}

static void
capacity_counter_release(LWLock *lock)
{
	UT_ASSERT(lock == &capacity_shared.lwlock);
	UT_ASSERT_EQ(capacity_lock_depth, 1);
	capacity_lock_depth--;
	capacity_releases++;
}

#define LWLockAcquire capacity_counter_acquire
#define LWLockRelease capacity_counter_release
#include "test_cluster_capacity_counter_srf.inc"
#undef LWLockAcquire
#undef LWLockRelease

static const char *const capacity_keys[] = {
	"tt_slot_active_full_wait_count", "tt_capacity_pool_wait_count",
	"tt_capacity_wakeup_recheck_count", "tt_capacity_cancel_count",
	"tt_capacity_refuse_by_reason.context", "tt_capacity_refuse_by_reason.proof",
	"capacity_wait_entered", "capacity_wait_repolled",
	"capacity_wait_refused_context", "capacity_wait_refused_proof",
	"undo_retain_by_reason.active_or_prepared", "undo_retain_by_reason.record_writer",
	"undo_retain_by_reason.seal_or_drain", "undo_retain_by_reason.horizon_missing",
	"undo_retain_by_reason.history_pin", "undo_retain_by_reason.reference_pending",
	"undo_retain_by_reason.release_not_durable", "undo_retain_by_reason.publisher_pending",
	"undo_retain_by_reason.identity_or_io"
};

static void
capacity_counter_reset(void)
{
	MemSet(&capacity_shared, 0, sizeof(capacity_shared));
	pg_atomic_init_u64(&capacity_shared.capacity_wait_refused_context, 0);
	pg_atomic_init_u64(&capacity_shared.capacity_wait_refused_proof, 0);
	pg_atomic_init_u64(&capacity_shared.capacity_wait_cancelled, 0);
	pg_atomic_init_u64(&capacity_shared.active_full_wakeup_recheck_count, 0);
	undo_cleaner_state = &capacity_shared;
	cluster_node_id = 3;
	capacity_acquires = capacity_releases = capacity_lock_depth = 0;
}

static void
capacity_counter_dump(bool available, const char *origin)
{
	LOCAL_FCINFO(fcinfo, 0);
	ReturnSetInfo rsinfo;
	unsigned before = capacity_acquires;

	MemSet(fcinfo, 0, SizeForFunctionCallInfo(0));
	MemSet(&rsinfo, 0, sizeof(rsinfo));
	captured_dump_row_count = captured_formatted_value_count = 0;
	fcinfo->resultinfo = (fmNodePtr)&rsinfo;
	(void)cluster_dump_state(fcinfo);
	UT_ASSERT_EQ(captured_dump_count("undo_cleaner", "tt_capacity_stats_available"), 1);
	UT_ASSERT_STR_EQ(captured_dump_value("undo_cleaner", "tt_capacity_stats_available"),
					 available ? "true" : "false");
	UT_ASSERT_EQ(captured_dump_count("undo_cleaner", "tt_capacity_origin_node_id"), available);
	if (available)
		UT_ASSERT_STR_EQ(captured_dump_value("undo_cleaner", "tt_capacity_origin_node_id"), origin);
	for (unsigned i = 0; i < lengthof(capacity_keys); i++)
		UT_ASSERT_EQ(captured_dump_count("undo_cleaner", capacity_keys[i]), available);
	UT_ASSERT_EQ(capacity_acquires - before, available ? 1 : 0);
	UT_ASSERT_EQ(capacity_releases, capacity_acquires);
	UT_ASSERT_EQ(capacity_lock_depth, 0);
}

static void
capacity_counter_expect(const char *key, const char *value)
{
	UT_ASSERT_EQ(captured_dump_count("undo_cleaner", key), 1);
	if (captured_dump_count("undo_cleaner", key) == 1)
		UT_ASSERT_STR_EQ(captured_dump_value("undo_cleaner", key), value);
}

UT_TEST(test_capacity_srf_distinguishes_detached_invalid_and_available_zero)
{
	static const int invalid[] = { INT_MIN, -1, 16, 17, 127, 128, INT_MAX };
	ClusterUndoCapacityStats out, before;

	capacity_counter_reset();
	MemSet(&out, 0xa5, sizeof(out));
	before = out;
	undo_cleaner_state = NULL;
	UT_ASSERT(!cluster_undo_cleaner_capacity_stats_snapshot(&out));
	UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
	capacity_counter_dump(false, NULL);
	cluster_undo_cleaner_note_capacity_refusal(true);
	cluster_undo_cleaner_note_tx_slot_recheck();
	UT_ASSERT_EQ(capacity_acquires, 0);
	undo_cleaner_state = &capacity_shared;
	for (unsigned i = 0; i < lengthof(invalid); i++) {
		cluster_node_id = invalid[i];
		UT_ASSERT(!cluster_undo_cleaner_capacity_stats_snapshot(&out));
		UT_ASSERT_EQ(memcmp(&out, &before, sizeof(out)), 0);
		capacity_counter_dump(false, NULL);
	}
	cluster_node_id = 0;
	UT_ASSERT(!cluster_undo_cleaner_capacity_stats_snapshot(NULL));
	capacity_counter_dump(true, "0");
	for (unsigned i = 0; i < lengthof(capacity_keys); i++)
		capacity_counter_expect(capacity_keys[i], "0");
	cluster_node_id = 15;
	capacity_counter_dump(true, "15");
}

UT_TEST(test_capacity_srf_preserves_unsigned_values_and_cv_alias)
{
	UndoCleanerSharedState before;

	capacity_counter_reset();
	capacity_shared.active_full_wait_count = UINT64_MAX;
	capacity_shared.capacity_wait_entered = UINT64_C(9223372036854775809);
	capacity_shared.capacity_wait_repolled = UINT64_C(9223372036854775809);
	pg_atomic_write_u64(&capacity_shared.active_full_wakeup_recheck_count, 7);
	pg_atomic_write_u64(&capacity_shared.capacity_wait_cancelled, UINT64_MAX);
	pg_atomic_write_u64(&capacity_shared.capacity_wait_refused_context, 19);
	pg_atomic_write_u64(&capacity_shared.capacity_wait_refused_proof, 23);
	before = capacity_shared;
	capacity_counter_dump(true, "3");
	capacity_counter_expect("tt_slot_active_full_wait_count", "18446744073709551615");
	capacity_counter_expect("tt_capacity_pool_wait_count", "9223372036854775809");
	capacity_counter_expect("tt_capacity_wakeup_recheck_count", "9223372036854775816");
	capacity_counter_expect("tt_capacity_cancel_count", "18446744073709551615");
	capacity_counter_expect("tt_capacity_refuse_by_reason.context", "19");
	capacity_counter_expect("tt_capacity_refuse_by_reason.proof", "23");
	capacity_counter_expect("capacity_wait_entered", "9223372036854775809");
	capacity_counter_expect("capacity_wait_repolled", "9223372036854775809");
	capacity_counter_expect("capacity_wait_refused_context", "19");
	capacity_counter_expect("capacity_wait_refused_proof", "23");
	UT_ASSERT_EQ(memcmp(&capacity_shared, &before, sizeof(before)), 0);
}

UT_TEST(test_capacity_srf_maps_all_retention_reasons_without_cross_attribution)
{
	capacity_counter_reset();
	for (unsigned reason = 0; reason < 9; reason++) {
		MemSet(capacity_shared.retain_by_reason, 0, sizeof(capacity_shared.retain_by_reason));
		capacity_shared.retain_by_reason[reason] = UINT64_MAX;
		capacity_counter_dump(true, "3");
		for (unsigned i = 0; i < 9; i++)
			capacity_counter_expect(capacity_keys[10 + i], i == reason ? "18446744073709551615" : "0");
	}
}

UT_TEST(test_capacity_srf_consumes_actual_atomic_hooks_without_changing_cv_counts)
{
	capacity_counter_reset();
	capacity_shared.capacity_wait_entered = 11;
	capacity_shared.capacity_wait_repolled = 5;
	cluster_undo_cleaner_note_capacity_refusal(true);
	cluster_undo_cleaner_note_capacity_refusal(false);
	cluster_undo_cleaner_note_capacity_refusal(false);
	for (unsigned i = 0; i < 7; i++)
		cluster_undo_cleaner_note_tx_slot_recheck();
	UT_ASSERT_EQ(capacity_acquires, 0);
	capacity_counter_dump(true, "3");
	capacity_counter_expect("tt_capacity_refuse_by_reason.context", "1");
	capacity_counter_expect("tt_capacity_refuse_by_reason.proof", "2");
	capacity_counter_expect("capacity_wait_refused_context", "1");
	capacity_counter_expect("capacity_wait_refused_proof", "2");
	capacity_counter_expect("tt_capacity_wakeup_recheck_count", "12");
	capacity_counter_expect("capacity_wait_repolled", "5");
	capacity_counter_expect("tt_capacity_pool_wait_count", "11");
	capacity_counter_expect("tt_slot_active_full_wait_count", "0");
	capacity_counter_expect("tt_capacity_cancel_count", "0");
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_capacity_srf_distinguishes_detached_invalid_and_available_zero);
	UT_RUN(test_capacity_srf_preserves_unsigned_values_and_cv_alias);
	UT_RUN(test_capacity_srf_maps_all_retention_reasons_without_cross_attribution);
	UT_RUN(test_capacity_srf_consumes_actual_atomic_hooks_without_changing_cv_counts);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
