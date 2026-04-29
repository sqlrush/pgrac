/*-------------------------------------------------------------------------
 *
 * test_cluster_ic_mock.c
 *	  Compile-time / link-level invariants for the cluster_ic mock
 *	  vtable shipped at stage 0.26.
 *
 *	  Locks:
 *	    - The CLUSTER_IC_TIER_MOCK enum value.
 *	    - The ClusterICOps_Mock vtable instance: tier_name, function
 *	      pointers non-NULL.
 *	    - Linkability of the four cluster_ic_mock_* SRF entry points
 *	      (the runtime SQL surface is verified end-to-end on a real
 *	      PG instance by cluster_tap t/014_ic_mock.pl).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ic_mock.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_ic.o standalone
 *	  pulls in references to ereport, pg_crc32c, cluster_node_id,
 *	  cluster_interconnect_tier, MemoryContext machinery, and the
 *	  funcapi / tuplestore / bytea machinery used by the SRF bodies.
 *	  The unit test stubs every one of these because it only takes
 *	  function addresses and reads constants -- no SRF body is ever
 *	  invoked from the unit test binary.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ic.h"

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
 * Stubs needed to link cluster_ic.o standalone.  Mirrors the set in
 * test_cluster_ic.c (stage 0.18); the mock additions only add
 * MemoryContextSwitchTo / palloc-via-cluster-context references that
 * unit_test.h's stub model already covers.
 * ----------
 */
#include "fmgr.h"
#include "funcapi.h"
#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/memutils.h"

int cluster_node_id = -1;
int cluster_interconnect_tier = 0;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

void
pg_usleep(long microsec pg_attribute_unused())
{}

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

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

extern pg_crc32c pg_comp_crc32c_sse42(pg_crc32c crc, const void *data, size_t len);
extern pg_crc32c pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len);

pg_crc32c
pg_comp_crc32c_sse42(pg_crc32c crc, const void *data pg_attribute_unused(),
					 size_t len pg_attribute_unused())
{
	return crc;
}

pg_crc32c
pg_comp_crc32c_armv8(pg_crc32c crc, const void *data pg_attribute_unused(),
					 size_t len pg_attribute_unused())
{
	return crc;
}

pg_crc32c (*pg_comp_crc32c)(pg_crc32c crc, const void *data, size_t len) = pg_comp_crc32c_sse42;

/*
 * MemoryContext / palloc / funcapi / tuplestore / bytea machinery used
 * by the mock vtable + SRF bodies.  Tests below take only function
 * addresses; bodies are dead, stubs satisfy the linker.
 * MemoryContextSwitchTo is a static inline in palloc.h so no stub
 * is needed for it.
 */
MemoryContext TopMemoryContext = NULL;
MemoryContext CurrentMemoryContext = NULL;

void *
palloc(Size size pg_attribute_unused())
{
	return NULL;
}

void *
palloc0(Size size pg_attribute_unused())
{
	return NULL;
}

void
pfree(void *pointer pg_attribute_unused())
{}

struct varlena *
pg_detoast_datum_packed(struct varlena *datum)
{
	return datum;
}

void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}

void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Enum + vtable invariants.
 * ============================================================ */

UT_TEST(test_mock_tier_enum_value)
{
	UT_ASSERT_EQ(CLUSTER_IC_TIER_MOCK, 4);
}

UT_TEST(test_mock_vtable_tier_name)
{
	UT_ASSERT_NOT_NULL(ClusterICOps_Mock.tier_name);
	UT_ASSERT_STR_EQ(ClusterICOps_Mock.tier_name, "mock");
}

UT_TEST(test_mock_vtable_send_nonnull)
{
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Mock.send_bytes);
}

UT_TEST(test_mock_vtable_recv_nonnull)
{
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Mock.recv_bytes);
}

UT_TEST(test_mock_vtable_init_nonnull)
{
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Mock.tier_init);
}

UT_TEST(test_mock_vtable_shutdown_nonnull)
{
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Mock.tier_shutdown);
}


/* ============================================================
 * SRF entry-point linkability.  The function bodies are #ifdef
 * USE_PGRAC_CLUSTER guarded; in disable-cluster builds they raise
 * ERRCODE_FEATURE_NOT_SUPPORTED.  This unit test is built in the
 * enable-cluster mode so the symbols resolve to the real bodies,
 * but we only take their addresses -- no fmgr machinery is exercised.
 * ============================================================ */

UT_TEST(test_mock_inject_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_mock_inject);
}

UT_TEST(test_mock_drain_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_mock_drain_outbound);
}

UT_TEST(test_mock_clear_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_mock_clear_all);
}

UT_TEST(test_mock_recv_test_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_mock_recv_test);
}


int
main(void)
{
	UT_PLAN(10);
	UT_RUN(test_mock_tier_enum_value);
	UT_RUN(test_mock_vtable_tier_name);
	UT_RUN(test_mock_vtable_send_nonnull);
	UT_RUN(test_mock_vtable_recv_nonnull);
	UT_RUN(test_mock_vtable_init_nonnull);
	UT_RUN(test_mock_vtable_shutdown_nonnull);
	UT_RUN(test_mock_inject_srf_linkable);
	UT_RUN(test_mock_drain_srf_linkable);
	UT_RUN(test_mock_clear_srf_linkable);
	UT_RUN(test_mock_recv_test_srf_linkable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
