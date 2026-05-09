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
DefineCustomBoolVariable(const char *name pg_attribute_unused(),
						 const char *short_desc pg_attribute_unused(),
						 const char *long_desc pg_attribute_unused(),
						 bool *valueAddr pg_attribute_unused(),
						 bool bootValue pg_attribute_unused(),
						 GucContext context pg_attribute_unused(), int flags pg_attribute_unused(),
						 GucBoolCheckHook check_hook pg_attribute_unused(),
						 GucBoolAssignHook assign_hook pg_attribute_unused(),
						 GucShowHook show_hook pg_attribute_unused())
{
	/* Stub: real impl in src/backend/utils/misc/guc.c.  Added at
	 * stage 1.2 for cluster.smgr_user_relations. */
}

/* Forward decl to silence -Wmissing-prototypes (stage 0.19). */
extern void cluster_conf_shmem_init(void);
extern Size cluster_conf_shmem_size(void);
extern void cluster_conf_load(void);

void
cluster_conf_shmem_init(void)
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

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	/* Cold-path version used by ereport(ERROR/FATAL/...) macros. */
	return false;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
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

/*
 * MemoryContext / palloc stubs for stage-1.3 registry path.  The unit
 * test never exercises cluster_init_shmem_module (would need full PG
 * memory machinery), so these only need to satisfy the linker.
 */
MemoryContext TopMemoryContext = NULL;

void *
MemoryContextAllocZero(MemoryContext context pg_attribute_unused(), Size size pg_attribute_unused())
{
	return NULL;
}

bool IsUnderPostmaster = false;

/* cluster_shmem_max_regions is provided by cluster_guc.o (linked in
 * by this test) -- no local definition. */

/* Assert() backstop: real impl in src/backend/utils/error/assert.c. */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
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

/*
 * Stage 1.7 stubs: cluster_pcm_lock.c (where cluster_pcm_grd_max_entries
 * lives and cluster_pcm_lock_module_init is defined) is not linked into
 * this test binary.  cluster_shmem.c references both symbols (via
 * cluster_init_shmem_module's pcm_grd region registration call).
 * Provide local stub bodies so linking succeeds; the stub variant of
 * cluster_pcm_lock_module_init is a no-op since this test never calls
 * cluster_init_shmem_module anyway.
 */
int cluster_pcm_grd_max_entries = 0;

void
cluster_pcm_lock_module_init(void)
{}


/*
 * Spec-1.10.1 D1 F1 stub: cluster_init_shmem_module also calls
 * cluster_phase_shmem_register (cluster_startup_phase.c).  Provide a
 * no-op stub since this test does not exercise the full module init.
 */
void
cluster_phase_shmem_register(void)
{}

/* Spec-1.11 Sprint A stub: cluster_init_shmem_module also calls
 * cluster_lmon_shmem_register (cluster_lmon.c). */
void
cluster_lmon_shmem_register(void)
{}

/* Spec-1.12 Sprint A stub: same for LCK. */
void
cluster_lck_shmem_register(void)
{}

/* Spec-1.13 Sprint A stub: same for DIAG. */
void
cluster_diag_shmem_register(void)
{}

/* Spec-1.14 Sprint A stub: same for Cluster Stats. */
void
cluster_stats_shmem_register(void)
{}

/* Spec-2.5 Sprint A stub: same for CSSD. */
void
cluster_cssd_shmem_register(void)
{}

/* Spec-1.15 Sprint A stub: same for SCN. */
void
cluster_scn_shmem_register(void)
{}

/* spec-2.2 D3 stub: tier1 shmem region (real impl in cluster_ic_tier1.c). */
void
cluster_ic_tier1_shmem_register(void)
{}

/* spec-2.4 D2 stub: cluster_epoch shmem region. */
void
cluster_epoch_shmem_register(void)
{}

/* spec-2.7 hardening F1 stub: cluster_smgr shmem region. */
void
cluster_smgr_shmem_register(void)
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


UT_TEST(test_cluster_shmem_size_matches_total_bytes)
{
	/*
	 * Stage 1.3: cluster_shmem_size() forwards to
	 * cluster_shmem_get_total_bytes().  In unit-test scope the
	 * registry is never built (no cluster_init_shmem_module call),
	 * so both must agree on 0.
	 */
	UT_ASSERT_EQ((int)cluster_shmem_size(), 0);
	UT_ASSERT_EQ((int)cluster_shmem_size(), (int)cluster_shmem_get_total_bytes());
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


/* ============================================================
 * Stage 1.3: region registry tests.
 *
 *	The runtime behavior (register / lookup / iter) requires PG's
 *	full memory + ereport machinery, so we cover that in the TAP
 *	test 020_shmem_registry.pl.  Here we focus on link-time +
 *	struct-shape invariants that hold without exercising registry
 *	state.
 * ============================================================ */

UT_TEST(test_cluster_shmem_region_struct_shape)
{
	/*
	 * Struct must be non-empty and big enough to hold the documented
	 * 6 fields (3 pointers + 1 int + 1 char* + 1 int).  A real-world
	 * size on 64-bit ABI is ~48 bytes including padding.
	 */
	UT_ASSERT(sizeof(ClusterShmemRegion) > 0);
	UT_ASSERT(sizeof(ClusterShmemRegion) >= 4 * sizeof(void *));
}


UT_TEST(test_cluster_shmem_registry_api_symbols_linkable)
{
	/*
	 * Stage 1.3 added 5 registry API entry points + 1 module init.
	 * Taking their addresses proves the symbols resolve.  We do NOT
	 * invoke any of them here -- runtime behavior is covered in the
	 * 020_shmem_registry.pl TAP test where PG memory + ereport
	 * machinery is real.
	 */
	UT_ASSERT_NOT_NULL((void *)cluster_shmem_register_region);
	UT_ASSERT_NOT_NULL((void *)cluster_shmem_lookup_region);
	UT_ASSERT_NOT_NULL((void *)cluster_shmem_iter_regions);
	UT_ASSERT_NOT_NULL((void *)cluster_shmem_get_region_count);
	UT_ASSERT_NOT_NULL((void *)cluster_shmem_get_total_bytes);
	UT_ASSERT_NOT_NULL((void *)cluster_init_shmem_module);
}


UT_TEST(test_cluster_shmem_get_region_count_pre_init)
{
	/*
	 * Before cluster_init_shmem_module() runs in this process the
	 * registry is NULL -> region_count is 0.
	 */
	UT_ASSERT_EQ(cluster_shmem_get_region_count(), 0);
}


UT_TEST(test_cluster_shmem_get_total_bytes_pre_init)
{
	/*
	 * Total bytes summed over an empty registry is 0.
	 */
	UT_ASSERT_EQ((int)cluster_shmem_get_total_bytes(), 0);
}


UT_TEST(test_cluster_shmem_lookup_region_returns_null_when_uninit)
{
	/*
	 * lookup before registry init should return NULL (defensive
	 * NULL-tolerance, not Assert).  This is documented behavior in
	 * cluster_shmem.h: "Returns NULL if not found".
	 */
	UT_ASSERT_NULL((void *)cluster_shmem_lookup_region("pgrac cluster control"));
	UT_ASSERT_NULL((void *)cluster_shmem_lookup_region(NULL));
}


UT_TEST(test_cluster_shmem_iter_regions_returns_false_when_uninit)
{
	/*
	 * iter before registry init should return false (no rows).
	 * Caller's *idx is unchanged on false return.
	 */
	int idx = 0;
	ClusterShmemRegion out;

	memset(&out, 0xff, sizeof(out));
	UT_ASSERT_EQ((int)cluster_shmem_iter_regions(&idx, &out), 0);
	UT_ASSERT_EQ(idx, 0);
}


UT_TEST(test_cluster_shmem_max_regions_default_value)
{
	/*
	 * The boot-value of cluster.shmem_max_regions is 64 (spec-1.3
	 * §2.2 / cluster_guc.c static initializer).  Unit test links
	 * cluster_guc.o but never calls cluster_init_guc, so the C
	 * global retains its static-initializer default.
	 */
	extern int cluster_shmem_max_regions;
	UT_ASSERT_EQ(cluster_shmem_max_regions, 64);
}


int
main(void)
{
	UT_PLAN(12);
	UT_RUN(test_cluster_shmem_magic_value);
	UT_RUN(test_cluster_shmem_ctl_size_within_budget);
	UT_RUN(test_cluster_shmem_size_matches_total_bytes);
	UT_RUN(test_cluster_shmem_pointer_defaults_null);
	UT_RUN(test_cluster_shmem_api_symbols_linkable);
	UT_RUN(test_cluster_shmem_region_struct_shape);
	UT_RUN(test_cluster_shmem_registry_api_symbols_linkable);
	UT_RUN(test_cluster_shmem_get_region_count_pre_init);
	UT_RUN(test_cluster_shmem_get_total_bytes_pre_init);
	UT_RUN(test_cluster_shmem_lookup_region_returns_null_when_uninit);
	UT_RUN(test_cluster_shmem_iter_regions_returns_false_when_uninit);
	UT_RUN(test_cluster_shmem_max_regions_default_value);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
