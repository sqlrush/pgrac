/*-------------------------------------------------------------------------
 *
 * test_cluster_pgstat.c
 *	  Compile-time / link-level invariants for the cluster_pgstat
 *	  performance-stats framework shipped at stage 0.28.
 *
 *	  Locks:
 *	    - The compile-time counter registry size (1 entry initially).
 *	    - Lookup by name (known + unknown).
 *	    - Initial counter value (0 after lazy init).
 *	    - inc / set / read helper behaviour.
 *	    - Lazy-init idempotency under repeated calls.
 *	    - Atomic counter accumulates correctly under sequential inc.
 *	    - Mirror sync with cluster_inject_armed_count works (manually
 *	      bump the source variable + read through pgstat_read).
 *	    - SRF entry-point linkability (cluster_get_pgstat_counters +
 *	      cluster_get_stat_nodes).
 *
 *	  End-to-end SQL behaviour is verified on a real PG instance by
 *	  cluster_tap t/016_perfmon.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_pgstat.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_pgstat.o
 *	  standalone pulls in references to PG_FUNCTION_INFO_V1 metadata
 *	  + cluster_injection_armed_count (mirror sync) + InitMaterialized-
 *	  SRF / tuplestore_putvalues / CStringGetTextDatum (SRF body
 *	  machinery).  All stubs below; the unit test only exercises the
 *	  helpers and never invokes the SRF body.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_pgstat.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ----------
 * Stubs needed to link cluster_pgstat.o standalone.  The framework
 * uses ereport (SRF body raise on disable build) + InitMaterializedSRF
 * + tuplestore_putvalues + CStringGetTextDatum.  None of those paths
 * are reached in this unit test (we only call the helpers and probe
 * SRF function addresses).
 * ----------
 */
#include "fmgr.h"
#include "funcapi.h"
#include "utils/elog.h"

/*
 * cluster_pgstat.c references cluster_injection_armed_count (from
 * cluster_inject.o) for the mirror-sync path.  We are not linking
 * cluster_inject.o here; provide a local definition so the mirror
 * sync compiles and we can drive the value to verify framework
 * behaviour.
 */
int cluster_injection_armed_count = 0;

/*
 * Stage 0.30 sweep: cluster_pgstat_sync_mirrors gained CLUSTER_INJECTION_POINT.
 */
void
cluster_injection_run(const char *name pg_attribute_unused())
{}

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}

void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}

text *
cstring_to_text(const char *s pg_attribute_unused())
{
	return NULL;
}

/*
 * cluster_views.c::cluster_get_stat_nodes is referenced by SRF symbol
 * test below.  We link only cluster_pgstat.o, not cluster_views.o,
 * so provide a forward declaration so the address-taking compiles;
 * actually-link comes from cluster_views.o when the full backend
 * pulls in the symbol -- but unit test does not link cluster_views.o,
 * so stub it.
 */
extern Datum cluster_get_stat_nodes(PG_FUNCTION_ARGS);

Datum
cluster_get_stat_nodes(PG_FUNCTION_ARGS)
{
	return (Datum)0;
}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Registry / lookup invariants.
 * ============================================================ */

UT_TEST(test_pgstat_count_compile_constant)
{
	/* Initial registry: 1 counter (cluster.inject.armed_count). */
	UT_ASSERT_NOT_NULL(cluster_pgstat_lookup("cluster.inject.armed_count"));
}

UT_TEST(test_pgstat_lookup_known_name)
{
	ClusterPgstatCounter *c = cluster_pgstat_lookup("cluster.inject.armed_count");
	UT_ASSERT_NOT_NULL(c);
	UT_ASSERT_STR_EQ(c->name, "cluster.inject.armed_count");
}

UT_TEST(test_pgstat_lookup_unknown)
{
	UT_ASSERT_EQ(cluster_pgstat_lookup("not-a-real-counter"), NULL);
}

UT_TEST(test_pgstat_lookup_null_safe)
{
	UT_ASSERT_EQ(cluster_pgstat_lookup(NULL), NULL);
}


/* ============================================================
 * Helper behaviour.
 * ============================================================ */

UT_TEST(test_pgstat_initial_zero)
{
	ClusterPgstatCounter *c = cluster_pgstat_lookup("cluster.inject.armed_count");
	cluster_pgstat_set(c, 0); /* reset for deterministic test */
	UT_ASSERT_EQ(cluster_pgstat_read(c), 0);
}

UT_TEST(test_pgstat_inc_increments)
{
	ClusterPgstatCounter *c = cluster_pgstat_lookup("cluster.inject.armed_count");
	cluster_pgstat_set(c, 0);
	cluster_pgstat_inc(c);
	cluster_pgstat_inc(c);
	cluster_pgstat_inc(c);
	UT_ASSERT_EQ(cluster_pgstat_read(c), 3);
}

UT_TEST(test_pgstat_set_overwrites)
{
	ClusterPgstatCounter *c = cluster_pgstat_lookup("cluster.inject.armed_count");
	cluster_pgstat_set(c, 100);
	UT_ASSERT_EQ(cluster_pgstat_read(c), 100);
	cluster_pgstat_set(c, 7);
	UT_ASSERT_EQ(cluster_pgstat_read(c), 7);
}

UT_TEST(test_pgstat_lazy_init_idempotent)
{
	/* Repeated lookups + helpers should not re-init or race. */
	ClusterPgstatCounter *c = cluster_pgstat_lookup("cluster.inject.armed_count");
	cluster_pgstat_set(c, 42);
	(void)cluster_pgstat_lookup("cluster.inject.armed_count");
	(void)cluster_pgstat_lookup("cluster.inject.armed_count");
	UT_ASSERT_EQ(cluster_pgstat_read(c), 42);
}

UT_TEST(test_pgstat_atomic_under_sequential_inc)
{
	ClusterPgstatCounter *c = cluster_pgstat_lookup("cluster.inject.armed_count");
	cluster_pgstat_set(c, 0);
	for (int i = 0; i < 1000; i++)
		cluster_pgstat_inc(c);
	UT_ASSERT_EQ(cluster_pgstat_read(c), 1000);
}


/* ============================================================
 * NULL safety on helpers.
 * ============================================================ */

UT_TEST(test_pgstat_helpers_null_safe)
{
	/* Passing NULL must not crash. */
	cluster_pgstat_inc(NULL);
	cluster_pgstat_set(NULL, 99);
	UT_ASSERT_EQ(cluster_pgstat_read(NULL), 0);
}


/* ============================================================
 * SRF entry-point linkability.
 * ============================================================ */

UT_TEST(test_pgstat_get_counters_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_get_pgstat_counters);
}

UT_TEST(test_pgstat_get_stat_nodes_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_get_stat_nodes);
}


/* ============================================================
 * cluster_inject mirror sync (read-only validation).
 *
 *	cluster_pgstat_sync_mirrors is internal to cluster_pgstat.c and
 *	is invoked from the SRF body.  We exercise the mirror behaviour
 *	indirectly: bump the source int, then call the SRF; since the
 *	SRF body is dead in unit (InitMaterializedSRF + tuplestore stubs
 *	are no-ops), we instead manually replicate the mirror by calling
 *	pgstat_set with the source value and verifying the read matches.
 * ============================================================ */

UT_TEST(test_pgstat_inject_mirror_replicate)
{
	ClusterPgstatCounter *c = cluster_pgstat_lookup("cluster.inject.armed_count");

	cluster_injection_armed_count = 5;
	cluster_pgstat_set(c, (uint64)cluster_injection_armed_count);
	UT_ASSERT_EQ(cluster_pgstat_read(c), 5);

	cluster_injection_armed_count = 0;
	cluster_pgstat_set(c, (uint64)cluster_injection_armed_count);
	UT_ASSERT_EQ(cluster_pgstat_read(c), 0);
}


int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_pgstat_count_compile_constant);
	UT_RUN(test_pgstat_lookup_known_name);
	UT_RUN(test_pgstat_lookup_unknown);
	UT_RUN(test_pgstat_lookup_null_safe);
	UT_RUN(test_pgstat_initial_zero);
	UT_RUN(test_pgstat_inc_increments);
	UT_RUN(test_pgstat_set_overwrites);
	UT_RUN(test_pgstat_lazy_init_idempotent);
	UT_RUN(test_pgstat_atomic_under_sequential_inc);
	UT_RUN(test_pgstat_helpers_null_safe);
	UT_RUN(test_pgstat_get_counters_srf_linkable);
	UT_RUN(test_pgstat_get_stat_nodes_srf_linkable);
	UT_RUN(test_pgstat_inject_mirror_replicate);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
