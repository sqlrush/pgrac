/*-------------------------------------------------------------------------
 *
 * test_cluster_guc.c
 *	  Compile-time and unit-level invariants for the cluster GUC
 *	  framework introduced in stage 0.13.
 *
 *	  Stage 0.13 wires the cluster GUC registration mechanism and
 *	  activates the first cluster GUC, cluster_node_id.  The full
 *	  DefineCustomIntVariable() registration depends on PG backend
 *	  symbols (guc.c machinery) that cluster_unit deliberately does
 *	  not link.  This test asserts only the structural pieces a
 *	  PG-free unit binary can observe:
 *
 *	  - The C global cluster_node_id exists at the address declared
 *	    by cluster_guc.h (proves the cluster_guc.o link target is
 *	    pulled into the standalone test binary).
 *	  - The boot-time default value is -1 ("unconfigured").
 *	  - cluster_init_guc is declared (forward declaration is enough --
 *	    we do not call it because it touches PG GUC machinery).
 *	  - cluster_phase remains a plain global owned by cluster_elog.c
 *	    (it is not migrated to a GUC; users do not set the lifecycle
 *	    phase).
 *
 *	  Runtime behavior of the GUC (SHOW/SET, pg_settings rows, range
 *	  validation, restart semantics) is validated separately by
 *	  cluster_tap t/007_guc.pl on a real PG instance.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_guc.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Includes postgres.h to pull in
 *	  basic PG types referenced by cluster_guc.h (none yet, but kept
 *	  for symmetry with peer tests), then undoes the printf -> pg_printf
 *	  redirection so the standalone unit test binary does not pull in
 *	  libpgport.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stdarg.h>

#include "cluster/cluster_conf.h" /* ClusterConf type for the D2b latch stub */
#include "cluster/cluster_guc.h"

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
 * Minimal PG stubs needed to link cluster_guc.o standalone.
 *
 *	cluster_guc.c references DefineCustomIntVariable(), which lives in
 *	the PG backend's GUC machinery and would drag in the entire backend
 *	if linked normally.  This unit test never calls cluster_init_guc(),
 *	so the stub body is never executed -- it only needs to satisfy the
 *	linker.  Runtime registration is exercised in cluster_tap on a real
 *	PG instance (see t/007_guc.pl).
 * ----------
 */
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
	/* Stub for unit-test linking; real impl lives in PG backend. */
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
	/* Stub for unit-test linking; real impl lives in PG backend. */
}

/* spec-5.12 D6: cluster_guc.c is the first cluster module to register real
 * (double) GUCs (the Hang Manager victim-score weights); stub it too. */
void
DefineCustomRealVariable(
	const char *name pg_attribute_unused(), const char *short_desc pg_attribute_unused(),
	const char *long_desc pg_attribute_unused(), double *valueAddr pg_attribute_unused(),
	double bootValue pg_attribute_unused(), double minValue pg_attribute_unused(),
	double maxValue pg_attribute_unused(), GucContext context pg_attribute_unused(),
	int flags pg_attribute_unused(), GucRealCheckHook check_hook pg_attribute_unused(),
	GucRealAssignHook assign_hook pg_attribute_unused(),
	GucShowHook show_hook pg_attribute_unused())
{
	/* Stub for unit-test linking; real impl lives in PG backend. */
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
	/* Stub for unit-test linking; real impl lives in PG backend. */
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
	/* Stub for unit-test linking; real impl lives in PG backend.
	 * Added at stage 1.2 for cluster.smgr_user_relations. */
}

/* spec-2.27 D4 stubs — GUC_check_errcode / GUC_check_errdetail macro
 * dependencies (pre_format_elog_string + format_elog_string +
 * GUC_check_errdetail_string global) so cluster_guc.o links standalone
 * even though check_hooks are never invoked in this unit test. */
char *GUC_check_errdetail_string = NULL;

/*
 * spec-3.18 D2b:  cluster_undo_buffer_writeback_check_hook references
 * ClusterConfShmem (via cluster_conf_has_peers) + ereport(WARNING) (errstart/
 * errfinish/errmsg/errhint/errdetail).  The hook is never invoked in this unit
 * test, so these are link-only stubs.
 */
ClusterConf *ClusterConfShmem = NULL;

int
cluster_conf_node_count(void)
{
	return 2;
}

int
pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
	int ret;
	va_list args;

	va_start(args, fmt);
	ret = vsnprintf(str, count, fmt, args);
	va_end(args);
	return ret;
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false; /* never starts an ereport in this test */
}
void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}
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
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
GUC_check_errcode(int sqlerrcode pg_attribute_unused())
{}

void
pre_format_elog_string(int errnum pg_attribute_unused(), const char *domain pg_attribute_unused())
{}

char *
format_elog_string(const char *fmt pg_attribute_unused(), ...)
{
	return NULL;
}

/*
 * cluster_init_guc references cluster_injection_assign_hook (from
 * cluster_inject.o) when registering cluster.injection_points (stage 0.27).
 * Stage 0.30 added a CLUSTER_INJECTION_POINT inside cluster_init_guc,
 * which expands (in --enable-cluster builds) to a global counter check
 * + a possible call to cluster_injection_run.  This unit test does not
 * link cluster_inject.o, so stub all three symbols.
 */
void
cluster_injection_assign_hook(const char *newval pg_attribute_unused(),
							  void *extra pg_attribute_unused())
{}

/* spec-5.10 — cluster_guc.o's ges_starvation_protection assign-hook flips the
 * GRD shared flag; cluster_grd.o is not linked here, so stub it. */
void
cluster_grd_set_starvation_protection(bool enabled pg_attribute_unused())
{}

int cluster_injection_armed_count = 0;

/*
 * Stage 1.7 stub: cluster_pcm_grd_max_entries lives in
 * cluster_pcm_lock.c which is not linked into this test binary
 * (would drag in PG runtime for ereport / LWLockInitialize).  We
 * provide a local definition matching the type so cluster_guc.c
 * (which references &cluster_pcm_grd_max_entries via
 * DefineCustomIntVariable) can link.
 *
 * spec-2.30 D5:  default changed 0 → -1 (sentinel auto-resolve to NBuffers
 * at startup).  Test stub mirrors real default for clarity;  cluster_guc.c
 * passes -1 as bootValue to DefineCustomIntVariable.
 */
int cluster_pcm_grd_max_entries = -1;

/* spec-5.51 stub: CR pool GUC storage lives in cluster_cr_pool.c, which is not
 * linked into this standalone test;  cluster_guc.c references the externs via
 * DefineCustomBool/IntVariable, so provide link-only stubs (mirror real defaults). */
bool cluster_shared_cr_pool_enabled = false;
int cluster_shared_cr_pool_size_blocks = 0;
int cluster_cr_pool_rel_generation_slots = 0; /* spec-5.56 D4 link-only stub */

/* spec-5.55 stub: resolver cache GUC storage lives in cluster_resolver_cache.c
 * (not linked here); cluster_guc.c references the externs via DefineCustom*. */
bool cluster_resolver_cache_enabled = false;
bool cluster_resolver_cache_measure = false;
int cluster_shared_resolver_cache_entries = 0;

/* spec-3.10 stub: cluster_cr_cache_max_blocks lives in cluster_cr_cache.c
 * (not linked here); cluster_guc.c references it via DefineCustomIntVariable. */
int cluster_cr_cache_max_blocks = 64;

/*
 * spec-5.51 + spec-5.52 backing-var stubs (standalone cluster_guc unit test,
 * held 5.51/5.52 stack).  cluster_guc.c registers GUCs whose backing vars live
 * in cluster_cr_pool.c (spec-5.51) and cluster_cr_admit.c (spec-5.52 D8),
 * neither of which is linked into this PG-free unit binary; provide local
 * storage so cluster_guc.o links standalone.  Link-only: this test never calls
 * cluster_init_guc(), so the values are never read.  NOT a substrate / shmem /
 * ClusterCRShared / production-semantics change -- pure test linkage.
 */
int cluster_cr_pool_admission_policy = 0;			/* spec-5.52 D8 (0 == admit_all) */
int cluster_cr_pool_admit_relation_backend_cap = 0; /* spec-5.52 D8 */
int cluster_cr_pool_admit_pressure_ratio = 0;		/* spec-5.52 D8 */
int cluster_cross_instance_cr_coordinator = 1;		/* spec-5.57 D3 (1 == boundary) */
bool cluster_cross_instance_cr_probe = false;		/* spec-5.57 D0 */

/* spec-5.1b D7: cluster_ges_mode_selfcheck GUC removed (cluster_guc.c no
 * longer references it), so the stub definition is gone too. */

void
cluster_injection_run(const char *name pg_attribute_unused())
{}


UT_DEFINE_GLOBALS();


/*
 * cluster_phase is declared in cluster/cluster_elog.h but we deliberately
 * avoid including it here -- this file's job is to verify that 0.13 left
 * cluster_phase as a plain (non-GUC) global.  Forward-declare the symbol
 * so we can take its address for a non-NULL assertion without coupling
 * to the elog header's full surface.
 */
extern const char *cluster_phase;


UT_TEST(test_cluster_node_id_default_is_minus_one)
{
	UT_ASSERT_EQ(cluster_node_id, -1);
}


UT_TEST(test_cluster_node_id_address_stable)
{
	/*
	 * Taking the address proves the linker resolved the symbol from
	 * cluster_guc.o (where the storage was relocated in stage 0.13)
	 * rather than from some stale copy in cluster_elog.o.  The exact
	 * value of the pointer is implementation-defined; we only assert
	 * it is non-null, which the C standard guarantees for any object.
	 */
	UT_ASSERT_NOT_NULL(&cluster_node_id);
}


UT_TEST(test_cluster_init_guc_symbol_is_linkable)
{
	/*
	 * cluster_init_guc() depends on PG backend symbols (DefineCustomIntVariable)
	 * which cluster_unit does not link, so we cannot call it.  Asserting that
	 * its address can be taken is enough to confirm the declaration is in
	 * cluster_guc.h and the function would link in a full backend build.
	 */
	UT_ASSERT_NOT_NULL((void *)cluster_init_guc);
}


UT_TEST(test_cluster_phase_remains_plain_global)
{
	/*
	 * cluster_phase is a lifecycle-state pointer, not a user-facing
	 * GUC.  Spec-1.10 (HC2) made it a read-only derived mirror of the
	 * ClusterStartupPhase enum, written ONLY by cluster_advance_phase()
	 * in cluster_startup_phase.c.  The boot value is the literal
	 * "pre_init" set in cluster_elog.c (matches CLUSTER_PHASE_PRE_INIT).
	 */
	UT_ASSERT_NOT_NULL(cluster_phase);
	UT_ASSERT_STR_EQ(cluster_phase, "pre_init");
}

UT_TEST(test_cluster_adg_guc_defaults)
{
	UT_ASSERT_EQ(cluster_dg_role, CLUSTER_DG_ROLE_PRIMARY);
	UT_ASSERT_EQ(cluster_dg_mode, CLUSTER_DG_MODE_ASYNC);
	UT_ASSERT_EQ((int)cluster_enable_adg, 0);
	UT_ASSERT_EQ((int)cluster_apply_master_election, 1);
	UT_ASSERT_EQ(cluster_adg_rfs_conninfos, NULL);
	UT_ASSERT_EQ(cluster_adg_lag_threshold_sec, 10);
	UT_ASSERT_EQ(cluster_max_standby_delay, 30);
	UT_ASSERT_EQ(cluster_apply_master_switch_drain_ms, 5000);
	UT_ASSERT_EQ(cluster_adg_barrier_interval_ms, 1000);
	UT_ASSERT_EQ(cluster_wal_sender_timeout_sec, 60);
	UT_ASSERT_EQ(cluster_wal_receiver_timeout_sec, 60);
}


int
main(void)
{
	UT_PLAN(5);
	UT_RUN(test_cluster_node_id_default_is_minus_one);
	UT_RUN(test_cluster_node_id_address_stable);
	UT_RUN(test_cluster_init_guc_symbol_is_linkable);
	UT_RUN(test_cluster_phase_remains_plain_global);
	UT_RUN(test_cluster_adg_guc_defaults);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
