/*-------------------------------------------------------------------------
 *
 * test_cluster_config_wrap_observe.c
 *    Read the original wrap-state atomics, including absent native shmem.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_config_wrap_observe.c
 * NOTES
 *    Original shared struct and accessor execute, with local storage only.
 *    No voting-disk, durable authority or live wrap completion is claimed.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "../../backend/cluster/cluster_xid_stripe_boot.c"
#undef printf
#include "unit_test.h"
UT_DEFINE_GLOBALS();

static ClusterXidStripeBootShmem region;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}

UT_TEST(absent_shmem_is_not_ready)
{
	bool pending = false;
	StripeBootShmem = NULL;
	UT_ASSERT(!cluster_xid_wrap_barrier_observe(&pending));
	UT_ASSERT(pending);
	UT_ASSERT(!cluster_xid_wrap_barrier_observe(NULL));
}

UT_TEST(original_marks_distinguish_pending_from_terminal)
{
	bool pending = true;
	StripeBootShmem = &region;
	pg_atomic_init_u32(&region.wrap_barrier_marked, 0);
	pg_atomic_init_u32(&region.wrap_barrier_done, 0);
	UT_ASSERT(cluster_xid_wrap_barrier_observe(&pending) && !pending);
	cluster_xid_wrap_barrier_set_marked();
	UT_ASSERT(cluster_xid_wrap_barrier_observe(&pending) && pending);
	cluster_xid_wrap_barrier_set_done();
	UT_ASSERT(cluster_xid_wrap_barrier_observe(&pending) && !pending);
	UT_ASSERT(cluster_xid_wrap_barrier_marked() && cluster_xid_wrap_barrier_passed());
}

UT_TEST(inconsistent_terminal_is_not_empty)
{
	bool pending = false;
	StripeBootShmem = &region;
	pg_atomic_init_u32(&region.wrap_barrier_marked, 0);
	pg_atomic_init_u32(&region.wrap_barrier_done, 1);
	UT_ASSERT(!cluster_xid_wrap_barrier_observe(&pending));
	UT_ASSERT(pending);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(absent_shmem_is_not_ready);
	UT_RUN(original_marks_distinguish_pending_from_terminal);
	UT_RUN(inconsistent_terminal_is_not_empty);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
