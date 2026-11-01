#include "postgres.h"
#include "cluster/cluster_undo_capacity.h"
#include "storage/ipc.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
static ClusterUndoCapacityChangeFence state;
static pg_on_exit_callback exit_callback;
static Datum exit_arg;
static unsigned exit_registrations;

void
on_shmem_exit(pg_on_exit_callback function, Datum arg)
{
	exit_callback = function;
	exit_arg = arg;
	exit_registrations++;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	fprintf(stderr, "%s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

static void
reset(void)
{
	cluster_undo_capacity_fence_init(&state);
	cluster_undo_capacity_fence_attach(&state);
}

UT_TEST(test_lazy_read_scopes_do_not_prevent_cross_pass_observation)
{
	uint64 sequence;
	bool first, second;

	reset();
	UT_ASSERT(cluster_undo_capacity_change_sample(&sequence));
	first = cluster_undo_capacity_write_scope_begin();
	second = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_scope_end(first);
	UT_ASSERT(cluster_undo_capacity_change_current(sequence));
	cluster_undo_capacity_write_scope_end(second);
	UT_ASSERT(cluster_undo_capacity_change_current(sequence));
}

UT_TEST(test_actual_mutation_invalidates_before_and_after_visibility)
{
	uint64 old, next;
	bool outer, retained_guard;

	reset();
	UT_ASSERT(cluster_undo_capacity_change_sample(&old));
	outer = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	UT_ASSERT(!cluster_undo_capacity_change_sample(&next));
	retained_guard = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 1);
	/* The outer wrapper can return while its original XCUR guard lives. */
	cluster_undo_capacity_write_scope_end(outer);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&next));
	cluster_undo_capacity_write_scope_end(retained_guard);
	UT_ASSERT(cluster_undo_capacity_change_sample(&next));
	UT_ASSERT(next > old);
	UT_ASSERT(!cluster_undo_capacity_change_current(old));
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 0);
}

UT_TEST(test_two_writers_never_use_sequence_parity_as_stability)
{
	uint64 sequence = 99;
	bool own;

	reset();
	/* Model a second process's real shared registration using the same
	 * atomic transition, while this process executes the real scope core. */
	pg_atomic_fetch_add_u64(&state.inflight_writers, 1);
	pg_atomic_fetch_add_u64(&state.change_sequence, 1);
	own = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 2);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
	cluster_undo_capacity_write_scope_end(own);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
	pg_atomic_fetch_add_u64(&state.change_sequence, 1);
	pg_atomic_fetch_sub_u64(&state.inflight_writers, 1);
	UT_ASSERT(cluster_undo_capacity_change_sample(&sequence));
}

UT_TEST(test_exhaustion_permanently_invalidates_without_refusing_writers)
{
	uint64 sequence;
	bool entered;

	reset();
	pg_atomic_write_u64(&state.change_sequence, UINT64_MAX - 1);
	entered = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	cluster_undo_capacity_write_scope_end(entered);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.change_sequence), UINT64_MAX);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 0);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
	entered = cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	cluster_undo_capacity_write_scope_end(entered);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.change_sequence), UINT64_MAX);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
}

UT_TEST(test_original_exit_callback_retires_only_own_registration_once)
{
	uint64 sequence;

	reset();
	(void)cluster_undo_capacity_write_scope_begin();
	cluster_undo_capacity_write_mark();
	(void)cluster_undo_capacity_write_scope_begin();
	/* Another process's registration and mutation remain outstanding. */
	pg_atomic_fetch_add_u64(&state.inflight_writers, 1);
	pg_atomic_fetch_add_u64(&state.change_sequence, 1);
	UT_ASSERT_EQ(exit_registrations, 1);
	UT_ASSERT(exit_callback != NULL);
	exit_callback(0, exit_arg);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 1);
	UT_ASSERT(!cluster_undo_capacity_change_sample(&sequence));
	exit_callback(0, exit_arg);
	UT_ASSERT_EQ(pg_atomic_read_u64(&state.inflight_writers), 1);
	pg_atomic_fetch_add_u64(&state.change_sequence, 1);
	pg_atomic_fetch_sub_u64(&state.inflight_writers, 1);
	UT_ASSERT(cluster_undo_capacity_change_sample(&sequence));
}

int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_lazy_read_scopes_do_not_prevent_cross_pass_observation);
	UT_RUN(test_actual_mutation_invalidates_before_and_after_visibility);
	UT_RUN(test_two_writers_never_use_sequence_parity_as_stability);
	UT_RUN(test_exhaustion_permanently_invalidates_without_refusing_writers);
	UT_RUN(test_original_exit_callback_retires_only_own_registration_once);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
