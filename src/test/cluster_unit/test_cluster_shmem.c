/*-------------------------------------------------------------------------
 *
 * test_cluster_shmem.c
 *	  Compile-time and link-level invariants for the cluster shmem
 *	  framework introduced in stage 0.14.
 *
 *	  Stage 0.14 wires cluster_request_shmem() / cluster_init_shmem()
 *	  into PG's two-phase shmem flow, allocating the ClusterShmemCtl
 *	  control block.  The actual ShmemInitStruct call requires PG
 *	  backend symbols (the shmem index, ShmemAlloc, etc.) which
 *	  cluster_unit deliberately does not link.  This test asserts the
 *	  structural pieces a PG-free unit binary can observe:
 *
 *	  - CLUSTER_SHMEM_MAGIC matches the expected "PGRC" little-endian
 *	    value (0x50475243).
 *	  - sizeof(ClusterShmemCtl) is non-zero and small (within 64 bytes,
 *	    the budget documented in cluster-shmem-design.md).
 *	  - cluster_shmem_size() reports a MAXALIGN'd byte count that
 *	    matches the struct size (today: only the control block).
 *	  - The ClusterShmem global pointer defaults to NULL before
 *	    cluster_init_shmem() runs in this process.
 *	  - The public API symbols cluster_request_shmem() and
 *	    cluster_init_shmem() resolve at link time (their bodies stay
 *	    unexecuted because we never call them in the unit test).
 *
 *	  Runtime behavior of the shmem region (allocation succeeded,
 *	  magic written, server healthy) is validated separately by
 *	  cluster_tap t/008_shmem.pl on a real PG instance.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_shmem.c
 *
 * NOTES
 *	  This is a pgrac-original file.  The link step must pull in
 *	  cluster_shmem.o (for ClusterShmem + cluster_shmem_size +
 *	  cluster_request_shmem + cluster_init_shmem), cluster_guc.o
 *	  (for cluster_node_id), cluster_elog.o (for cluster_phase, used
 *	  by CLUSTER_LOG inside cluster_shmem.c), and cluster_version.o.
 *	  The unit test stubs the PG backend symbols that cluster_shmem.c
 *	  references but never executes (RequestAddinShmemSpace,
 *	  ShmemInitStruct, GetCurrentTimestamp).  See cluster_unit/Makefile.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_shmem.h"

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
 * Minimal PG stubs needed to link cluster_shmem.o standalone.
 *
 *	cluster_shmem.c references three PG backend functions that pull
 *	in the rest of the backend if linked normally.  The unit test
 *	never calls cluster_init_shmem(), so the stub bodies are never
 *	executed.  Real behavior is exercised by cluster_tap t/008_shmem.pl.
 *
 *	We also stub elog so CLUSTER_LOG inside cluster_shmem.c links
 *	even though it is never reached.
 * ----------
 */
#include "storage/shmem.h"
#include "utils/timestamp.h"

void
RequestAddinShmemSpace(Size size pg_attribute_unused())
{
	/* Stub: real impl in src/backend/storage/ipc/ipci.c */
}

#include "utils/guc.h"

void
DefineCustomIntVariable(const char *name pg_attribute_unused(),
						const char *short_desc pg_attribute_unused(),
						const char *long_desc pg_attribute_unused(),
						int *valueAddr pg_attribute_unused(), int bootValue pg_attribute_unused(),
						int minValue pg_attribute_unused(), int maxValue pg_attribute_unused(),
						GucContext context pg_attribute_unused(), int flags pg_attribute_unused(),
						GucIntCheckHook check_hook pg_attribute_unused(),
						GucIntAssignHook assign_hook pg_attribute_unused(),
						GucShowHook show_hook pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/misc/guc.c */
}

void
DefineCustomEnumVariable(const char *name pg_attribute_unused(),
						 const char *short_desc pg_attribute_unused(),
						 const char *long_desc pg_attribute_unused(),
						 int *valueAddr pg_attribute_unused(), int bootValue pg_attribute_unused(),
						 const struct config_enum_entry *options pg_attribute_unused(),
						 GucContext context pg_attribute_unused(), int flags pg_attribute_unused(),
						 GucEnumCheckHook check_hook pg_attribute_unused(),
						 GucEnumAssignHook assign_hook pg_attribute_unused(),
						 GucShowHook show_hook pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/misc/guc.c (stage 0.18) */
}

void
DefineCustomStringVariable(
	const char *name pg_attribute_unused(), const char *short_desc pg_attribute_unused(),
	const char *long_desc pg_attribute_unused(), char **valueAddr pg_attribute_unused(),
	const char *bootValue pg_attribute_unused(), GucContext context pg_attribute_unused(),
	int flags pg_attribute_unused(), GucStringCheckHook check_hook pg_attribute_unused(),
	GucStringAssignHook assign_hook pg_attribute_unused(),
	GucShowHook show_hook pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/misc/guc.c (stage 0.19) */
}

void
DefineCustomBoolVariable(
	const char *name pg_attribute_unused(), const char *short_desc pg_attribute_unused(),
	const char *long_desc pg_attribute_unused(), bool *valueAddr pg_attribute_unused(),
	bool bootValue pg_attribute_unused(), GucContext context pg_attribute_unused(),
	int flags pg_attribute_unused(), GucBoolCheckHook check_hook pg_attribute_unused(),
	GucBoolAssignHook assign_hook pg_attribute_unused(),
	GucShowHook show_hook pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/misc/guc.c.  Added at
	 * stage 1.2 for cluster.smgr_user_relations. */
}

/* Forward decl to silence -Wmissing-prototypes (stage 0.19). */
extern void cluster_conf_shmem_init(void);
extern void cluster_conf_shmem_request(void);
extern Size cluster_conf_shmem_size(void);
extern void cluster_conf_load(void);

void
cluster_conf_shmem_init(void)
{
	/* Stub: real impl in src/backend/cluster/cluster_conf.c (stage 0.19) */
}

void
cluster_conf_shmem_request(void)
{
	/* Stub: real impl in src/backend/cluster/cluster_conf.c (stage 0.19) */
}

Size
cluster_conf_shmem_size(void)
{
	/* Stub: real impl in src/backend/cluster/cluster_conf.c (stage 0.19) */
	return 0;
}

void
cluster_conf_load(void)
{
	/* Stub: real impl in src/backend/cluster/cluster_conf.c (stage 0.19) */
}

/* Forward decl to silence -Wmissing-prototypes. */
extern void cluster_ic_init(void);

void
cluster_ic_init(void)
{
	/* Stub: real impl in src/backend/cluster/cluster_ic.c (stage 0.18) */
}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr pg_attribute_unused())
{
	/* Stub: real impl in src/backend/storage/ipc/shmem.c */
	return NULL;
}

TimestampTz
GetCurrentTimestamp(void)
{
	/* Stub: real impl in src/backend/utils/adt/timestamp.c */
	return 0;
}

/*
 * Stubs for the ereport/errmsg/errfinish/errstart family used by
 * CLUSTER_LOG inside cluster_shmem.c.  The unit test never reaches
 * CLUSTER_LOG because we never call cluster_init_shmem(), so the stub
 * bodies are dead.  They only need to satisfy the linker.
 */
#include "utils/elog.h"

#include <stdarg.h>

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false; /* tell errmsg_internal/errfinish to skip */
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	/* never called when errstart returned false, but stub anyway */
}

/* add_size is in shmem.c (PG backend) and unconditionally pulled in by
 * cluster_shmem.o when it accumulates region sizes.  Provide a stub. */
Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

/*
 * Stage 0.27 injection-framework symbols used by cluster_shmem.o
 * (cluster_init_shmem expands CLUSTER_INJECTION_POINT macros).
 * cluster_inject.o is not linked here, so stub the symbols.
 */
int cluster_injection_armed_count = 0;

void
cluster_injection_run(const char *name pg_attribute_unused())
{}

void
cluster_injection_assign_hook(const char *newval pg_attribute_unused(),
							  void *extra pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


UT_TEST(test_cluster_shmem_magic_value)
{
	UT_ASSERT_EQ(CLUSTER_SHMEM_MAGIC, 0x50475243U);
}


UT_TEST(test_cluster_shmem_ctl_size_within_budget)
{
	/*
	 * The struct must be non-empty, and small enough that the cluster
	 * control block itself does not exceed the 64-byte budget
	 * documented in docs/cluster-shmem-design.md §3.1.
	 */
	UT_ASSERT(sizeof(ClusterShmemCtl) > 0);
	UT_ASSERT(sizeof(ClusterShmemCtl) <= 64);
}


UT_TEST(test_cluster_shmem_size_matches_struct)
{
	/*
	 * cluster_shmem_size() should return MAXALIGN(sizeof(ctl)) at this
	 * stage (only the control block is registered).  We do not call
	 * MAXALIGN() here -- it is a PG macro that depends on backend
	 * settings -- but the result must be at least the struct size.
	 */
	UT_ASSERT(cluster_shmem_size() >= sizeof(ClusterShmemCtl));
}


UT_TEST(test_cluster_shmem_pointer_defaults_null)
{
	/*
	 * Before cluster_init_shmem() runs in this process, the global
	 * pointer must be NULL.  The stub for ShmemInitStruct returns NULL
	 * too, so even after a hypothetical call the assertion would hold,
	 * but we deliberately do NOT call cluster_init_shmem() here.
	 */
	UT_ASSERT_NULL(ClusterShmem);
}


UT_TEST(test_cluster_shmem_api_symbols_linkable)
{
	/*
	 * Taking the address of each public entry point proves the
	 * symbols resolve.  We never call them in this binary because
	 * doing so would exercise the PG stubs above and prove nothing.
	 */
	UT_ASSERT_NOT_NULL((void *)cluster_request_shmem);
	UT_ASSERT_NOT_NULL((void *)cluster_init_shmem);
	UT_ASSERT_NOT_NULL((void *)cluster_shmem_size);
}


int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_cluster_shmem_magic_value);
	UT_RUN(test_cluster_shmem_ctl_size_within_budget);
	UT_RUN(test_cluster_shmem_size_matches_struct);
	UT_RUN(test_cluster_shmem_pointer_defaults_null);
	UT_RUN(test_cluster_shmem_api_symbols_linkable);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
