/* Native H/source admission uses the real ProcArray owner bodies.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "port/atomics.h"
static bool k_history_cas(volatile pg_atomic_uint32 *ptr, uint32 *expected, uint32 value);
#define pg_atomic_compare_exchange_u32 k_history_cas
int k_history_inherited_main(void);
#define main k_history_inherited_main
#include "test_cluster_retention_sample.c"
#undef main
#include "test_cluster_retention_history.inc"
#undef pg_atomic_compare_exchange_u32

/* Deterministic scheduling at the real atomic boundary, never a stand-in H
 * authority. Every scheduled cut and source call executes its product body. */
static int k_history_schedule;
static bool k_history_racing_source;
static bool k_history_racing_cut;
static unsigned k_history_cas_calls;
static bool
k_history_cas(volatile pg_atomic_uint32 *ptr, uint32 *expected, uint32 value)
{
	int schedule = k_history_schedule;
	bool result;

	k_history_cas_calls++;
	k_history_schedule = 0;
	if (schedule == 1 || schedule == 3)
		UT_ASSERT(cluster_undo_retention_unknown_source_enter(&k_history_racing_source));
	else if (schedule == 2) {
		ClusterUndoRetentionSample cut;

		k_history_racing_cut = cluster_undo_retention_horizon_sample(&cut);
	}
	result = pg_atomic_compare_exchange_u32(ptr, expected, value);
	if (schedule == 4) {
		UT_ASSERT(result && value == UINT32_MAX);
		UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&k_history_racing_source));
	}
	return result;
}

static void
k_history_boot(void)
{
	reset_fixture();
	pg_atomic_init_u32(&procArray->cluster_unknown_history_sources, 0);
}

UT_TEST(k_history_source_blocks_cut_until_confirmed_exit)
{
	ClusterUndoRetentionSample cut;
	bool first = false, second = false;

	k_history_boot();
	UT_ASSERT(cluster_undo_retention_unknown_source_enter(&first));
	UT_ASSERT(cluster_undo_retention_unknown_source_enter(&second));
	UT_ASSERT(first && second);
	UT_ASSERT_EQ(acquires, 0);
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&cut));
	UT_ASSERT_EQ(cut.horizon_scn, InvalidScn);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, InvalidScn);
	UT_ASSERT(cluster_undo_retention_unknown_source_leave(&first));
	UT_ASSERT(!first && second);
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&cut));
	UT_ASSERT(cluster_undo_retention_unknown_source_leave(&second));
	UT_ASSERT(!second);
	UT_ASSERT(cluster_undo_retention_horizon_sample(&cut));
	UT_ASSERT_EQ(cut.horizon_scn, 700);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(k_history_cut_blocks_later_unknown_source_without_locking)
{
	ClusterUndoRetentionSample cut;
	bool registered = false;
	unsigned before;

	k_history_boot();
	UT_ASSERT(cluster_undo_retention_horizon_sample(&cut));
	before = acquires;
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&registered));
	UT_ASSERT(!registered);
	UT_ASSERT_EQ(acquires, before);
	UT_ASSERT_EQ(pg_atomic_read_u32(&procArray->cluster_unknown_history_sources), 0);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
	UT_ASSERT(!cluster_undo_retention_unknown_source_leave(&registered));
	UT_ASSERT(cluster_undo_retention_horizon_covers(700));
}

UT_TEST(k_history_commit_reservation_and_capacity_never_spin_or_clear)
{
	ClusterUndoRetentionSample cut;
	bool registered = false;

	k_history_boot();
	pg_atomic_write_u32(&procArray->cluster_unknown_history_sources, UINT32_MAX);
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&registered));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&cut));
	UT_ASSERT_EQ(pg_atomic_read_u32(&procArray->cluster_unknown_history_sources), UINT32_MAX);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, InvalidScn);
	pg_atomic_write_u32(&procArray->cluster_unknown_history_sources, UINT32_MAX - 1);
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&registered));
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&cut));
	UT_ASSERT_EQ(pg_atomic_read_u32(&procArray->cluster_unknown_history_sources), UINT32_MAX - 1);
	UT_ASSERT(!registered);
	UT_ASSERT_EQ(lock_errors, 0);
}

UT_TEST(k_history_missing_owner_and_invalid_token_preserve_responsibility)
{
	ProcArrayStruct *saved;
	bool registered = false;

	k_history_boot();
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(NULL));
	UT_ASSERT(!cluster_undo_retention_unknown_source_leave(NULL));
	UT_ASSERT(!cluster_undo_retention_unknown_source_leave(&registered));
	UT_ASSERT(cluster_undo_retention_unknown_source_enter(&registered));
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&registered));
	UT_ASSERT_EQ(pg_atomic_read_u32(&procArray->cluster_unknown_history_sources), 1);
	saved = procArray;
	procArray = NULL;
	UT_ASSERT(!cluster_undo_retention_unknown_source_leave(&registered));
	UT_ASSERT(registered);
	procArray = saved;
	UT_ASSERT(cluster_undo_retention_unknown_source_leave(&registered));
	UT_ASSERT(!cluster_undo_retention_unknown_source_leave(&registered));
	cluster_node_id = -1;
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&registered));
	UT_ASSERT_EQ(acquires, 0);
}

UT_TEST(k_history_nonfinite_and_regressing_cut_release_own_reservation)
{
	ClusterUndoRetentionSample cut;
	bool registered = false;

	k_history_boot();
	procArray->cluster_read_scn_floor = (SCN) UINT64_MAX;
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&registered));
	UT_ASSERT(!registered);
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&cut));
	UT_ASSERT_EQ(pg_atomic_read_u32(&procArray->cluster_unknown_history_sources), 0);
	procArray->cluster_read_scn_floor = 800;
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&cut));
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 800);
	UT_ASSERT_EQ(pg_atomic_read_u32(&procArray->cluster_unknown_history_sources), 0);
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&registered));
}

UT_TEST(k_history_source_and_cut_interleavings_share_one_admission_order)
{
	ClusterUndoRetentionSample cut;
	bool registered = false;

	k_history_boot();
	k_history_schedule = 1;
	k_history_racing_source = false;
	UT_ASSERT(!cluster_undo_retention_horizon_sample(&cut));
	UT_ASSERT(k_history_racing_source);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, InvalidScn);
	UT_ASSERT(cluster_undo_retention_unknown_source_leave(&k_history_racing_source));
	UT_ASSERT(cluster_undo_retention_horizon_sample(&cut));

	k_history_boot();
	k_history_schedule = 2;
	k_history_racing_cut = false;
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&registered));
	UT_ASSERT(k_history_racing_cut && !registered);
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
	UT_ASSERT_EQ(pg_atomic_read_u32(&procArray->cluster_unknown_history_sources), 0);
}

UT_TEST(k_history_cas_contention_and_commit_owner_refuse_without_retry)
{
	ClusterUndoRetentionSample cut;
	bool registered = false;
	unsigned calls;

	k_history_boot();
	k_history_racing_source = false;
	k_history_schedule = 3;
	calls = k_history_cas_calls;
	UT_ASSERT(!cluster_undo_retention_unknown_source_enter(&registered));
	UT_ASSERT(k_history_racing_source && !registered);
	UT_ASSERT_EQ(k_history_cas_calls - calls, 2); /* one call per contender */
	UT_ASSERT_EQ(pg_atomic_read_u32(&procArray->cluster_unknown_history_sources), 1);
	UT_ASSERT(cluster_undo_retention_unknown_source_leave(&k_history_racing_source));
	k_history_schedule = 4;
	calls = k_history_cas_calls;
	UT_ASSERT(cluster_undo_retention_horizon_sample(&cut));
	UT_ASSERT(!k_history_racing_source);
	UT_ASSERT_EQ(k_history_cas_calls - calls, 1); /* busy entrant makes no CAS */
	UT_ASSERT_EQ(procArray->cluster_read_scn_floor, 700);
	UT_ASSERT_EQ(lock_errors, 0);
}

int
main(void)
{
	UT_PLAN(7);
	UT_RUN(k_history_source_blocks_cut_until_confirmed_exit);
	UT_RUN(k_history_cut_blocks_later_unknown_source_without_locking);
	UT_RUN(k_history_commit_reservation_and_capacity_never_spin_or_clear);
	UT_RUN(k_history_missing_owner_and_invalid_token_preserve_responsibility);
	UT_RUN(k_history_nonfinite_and_regressing_cut_release_own_reservation);
	UT_RUN(k_history_source_and_cut_interleavings_share_one_admission_order);
	UT_RUN(k_history_cas_contention_and_commit_owner_refuse_without_retry);
	free(copy_storage);
	free(procArray);
	UT_DONE();
	return ut_failed_count != 0;
}
