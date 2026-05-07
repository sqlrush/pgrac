/*-------------------------------------------------------------------------
 *
 * test_cluster_ic.c
 *	  Compile-time / link-level invariants for the cluster internal
 *	  IPC abstraction layer introduced in stage 0.18.
 *
 *	  Stage 0.18 ships only the stub interconnect tier: target == self
 *	  is a no-op success, target != self ereports
 *	  ERRCODE_FEATURE_NOT_SUPPORTED.  Real send/recv round-trips are
 *	  verified at the SQL level by cluster_tap t/012_ic.pl on a
 *	  running PG instance; this unit test only locks the wire-format
 *	  size, the magic / protocol_version constants, and the symbol
 *	  surface that Stage 2+ subsystems will link against.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ic.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Linking cluster_ic.o standalone
 *	  pulls in references to ereport / pg_crc32c / cluster_node_id;
 *	  those are stubbed locally below so the binary can run without
 *	  the full PG backend.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ic.h"

/*
 * postgres.h transitively pulls in port.h which redirects printf etc.
 * Standalone unit-test binaries do not link libpgport, so undo the
 * redirection before pulling in unit_test.h.
 */
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
 * Stubs needed to link cluster_ic.o standalone.
 *
 *	cluster_ic.c calls into pg_crc32c (provided by libpgport, not
 *	linked here) and references cluster_node_id (defined in
 *	cluster_guc.o, not linked here).  We provide minimal local
 *	definitions; these tests only check linkability and constants,
 *	so the stub return values are inert.
 *
 *	cluster_ic.c also calls ereport on the !self path; ereport is
 *	macro-expanded into errstart/errmsg/errfinish.  We provide minimal
 *	stubs for those so the linker is satisfied.  The tests below never
 *	invoke a path that triggers ereport.
 *
 *	Stage 0.26 added the mock vtable + four cluster_ic_mock_* SRFs to
 *	cluster_ic.o; their bodies reference palloc / pfree / TopMemoryContext
 *	/ pg_detoast_datum_packed / InitMaterializedSRF / tuplestore_putvalues.
 *	None of those paths are exercised here, so the additional stubs are
 *	inert.
 * ----------
 */
#include "fmgr.h"
#include "funcapi.h"
#include "utils/elog.h"
#include "utils/memutils.h"

int cluster_node_id = -1;
int cluster_interconnect_tier = 0; /* CLUSTER_IC_TIER_STUB */

/*
 * spec-2.1 Hardening v1.0.2 D-I1 -- F2 extension fix in cluster_ic.c::
 * cluster_ic_init adds an extern reference to cluster_enabled
 * (defensive guard mirroring the cluster_conf_load v1.0.1 pattern).
 * Stub matches GUC default; tests do not exercise the
 * !cluster_enabled early-return path (verified at TAP layer L12).
 */
bool cluster_enabled = true;

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/error/assert.c */
	abort();
}

void
pg_usleep(long microsec pg_attribute_unused())
{
	/* Stub: real impl in src/port/pgsleep.c.  Tests never reach RPC timeout. */
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	/* Stub: matches PG's cold path wrapper around errstart */
	return false;
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/error/elog.c */
}

/*
 * pg_crc32c hardware-accelerated functions.  cluster_ic.c uses
 * COMP_CRC32C which expands to one of these depending on platform
 * (USE_ARMV8_CRC32C on macOS arm64, USE_SSE42_CRC32C on Linux x86_64).
 * The unit test never invokes cluster_msg_send (which is what would
 * trigger the CRC compute), but the linker still pulls in the symbol
 * via cluster_ic.o.  Provide both stubs so the binary links on either
 * platform; the runtime-check variants use a function pointer
 * (pg_comp_crc32c) which is not stubbed because PG configure on the
 * platforms that pgrac targets selects the direct-call form.
 *
 * pg_crc32c.h only declares the prototype for whichever variant the
 * current platform uses (#ifdef USE_*_CRC32C); we forward-declare both
 * unconditionally so that defining both is portable across platforms.
 */
extern pg_crc32c pg_comp_crc32c_sse42(pg_crc32c crc, const void *data, size_t len);
extern pg_crc32c pg_comp_crc32c_armv8(pg_crc32c crc, const void *data, size_t len);

pg_crc32c
pg_comp_crc32c_sse42(pg_crc32c crc, const void *data pg_attribute_unused(),
					 size_t len pg_attribute_unused())
{
	/* Stub: real impl in src/port/pg_crc32c_sse42.c */
	return crc;
}

pg_crc32c
pg_comp_crc32c_armv8(pg_crc32c crc, const void *data pg_attribute_unused(),
					 size_t len pg_attribute_unused())
{
	/* Stub: real impl in src/port/pg_crc32c_armv8.c */
	return crc;
}

/*
 * Linux x86_64 PG configure picks USE_SSE42_CRC32C_WITH_RUNTIME_CHECK,
 * making COMP_CRC32C expand to a call through the pg_comp_crc32c
 * function pointer.  The real backend initialises this pointer in
 * src/port/pg_crc32c_sse42_choose.c at startup; the unit test never
 * triggers the path, but the linker still needs the variable to
 * resolve.  Initialise to the local sse42 stub above for safety.
 */
pg_crc32c (*pg_comp_crc32c)(pg_crc32c crc, const void *data, size_t len) = pg_comp_crc32c_sse42;

/*
 * Stage 0.26 mock-vtable / mock-SRF dependencies.  Tests below take
 * only addresses; bodies never run, stubs satisfy the linker.
 * MemoryContextSwitchTo is a static inline in palloc.h so no stub is
 * needed for it.
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

/*
 * Stage 0.27 injection-framework symbols used by cluster_ic.o
 * (cluster_ic_init + mock_send_bytes expand CLUSTER_INJECTION_POINT).
 * cluster_inject.o is not linked here; stub the symbols.
 */
int cluster_injection_armed_count = 0;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


/* ============================================================
 * Wire format invariants (compile-time anchors).
 * ============================================================ */

UT_TEST(test_msg_header_size_24)
{
	UT_ASSERT_EQ(sizeof(ClusterMsgHeader), 24);
	UT_ASSERT_EQ(PGRAC_IC_HEADER_BYTES, 24);
}

UT_TEST(test_msg_header_magic_constant)
{
	/* "ICRG" little-endian = 0x47435249 */
	UT_ASSERT_EQ(PGRAC_IC_MAGIC, (uint32)0x47435249);
}

UT_TEST(test_msg_header_protocol_v1)
{
	UT_ASSERT_EQ(PGRAC_IC_PROTOCOL_VERSION_V1, 1);
}


/* ============================================================
 * Symbol linkability -- guarantees Stage 2+ subsystems will find
 * the API they expect.
 * ============================================================ */

UT_TEST(test_ic_send_bytes_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_send_bytes);
}

UT_TEST(test_ic_recv_bytes_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_recv_bytes);
}

UT_TEST(test_msg_send_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_msg_send);
}

UT_TEST(test_msg_recv_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_msg_recv);
}

UT_TEST(test_rpc_call_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_rpc_call);
}

UT_TEST(test_ic_init_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_init);
}

UT_TEST(test_ic_shutdown_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_ic_shutdown);
}


/* ============================================================
 * Stub vtable contract.
 * ============================================================ */

UT_TEST(test_stub_vtable_tier_name)
{
	UT_ASSERT_NOT_NULL(ClusterICOps_Stub.tier_name);
	UT_ASSERT_STR_EQ(ClusterICOps_Stub.tier_name, "stub");
}

UT_TEST(test_stub_vtable_send_nonnull)
{
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Stub.send_bytes);
}

UT_TEST(test_stub_vtable_recv_nonnull)
{
	UT_ASSERT_NOT_NULL((void *)ClusterICOps_Stub.recv_bytes);
}


int
main(void)
{
	UT_PLAN(13);
	UT_RUN(test_msg_header_size_24);
	UT_RUN(test_msg_header_magic_constant);
	UT_RUN(test_msg_header_protocol_v1);
	UT_RUN(test_ic_send_bytes_linkable);
	UT_RUN(test_ic_recv_bytes_linkable);
	UT_RUN(test_msg_send_linkable);
	UT_RUN(test_msg_recv_linkable);
	UT_RUN(test_rpc_call_linkable);
	UT_RUN(test_ic_init_linkable);
	UT_RUN(test_ic_shutdown_linkable);
	UT_RUN(test_stub_vtable_tier_name);
	UT_RUN(test_stub_vtable_send_nonnull);
	UT_RUN(test_stub_vtable_recv_nonnull);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
