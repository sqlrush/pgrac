/*-------------------------------------------------------------------------
 *
 * test_cluster_phase_barriers.h
 *    Test-only scheduling boundaries around the original memory barriers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_phase_barriers.h
 *
 * NOTES
 *    PGRAC original. Included only when compiling the instrumented unit object.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "port/atomics.h"

extern void phase_fixture_read_barrier_hook(void);
extern void phase_fixture_write_barrier_hook(void);

static inline void
phase_fixture_read_barrier(void)
{
	pg_read_barrier();
	phase_fixture_read_barrier_hook();
}

static inline void
phase_fixture_write_barrier(void)
{
	pg_write_barrier();
	phase_fixture_write_barrier_hook();
}

#undef pg_read_barrier
#undef pg_write_barrier
#define pg_read_barrier() phase_fixture_read_barrier()
#define pg_write_barrier() phase_fixture_write_barrier()
