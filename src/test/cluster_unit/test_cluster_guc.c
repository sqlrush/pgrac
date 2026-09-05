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
#include <sys/un.h>

#include "common/relpath.h"
#include "cluster/cluster_conf.h" /* ClusterConf type for the D2b latch stub */
#include "cluster/cluster_guc.h"
#include "storage/block.h"

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

extern int cluster_undo_buffers;

static int *undo_buffers_value_addr = NULL;
static int undo_buffers_boot_value = -1;
static int undo_buffers_min_value = -1;
static int undo_buffers_max_value = -1;
static GucContext undo_buffers_context = PGC_INTERNAL;
static const char *undo_buffers_long_desc = NULL;
static int *external_fence_timeout_value_addr = NULL;
static int external_fence_timeout_boot_value = -1;
static int external_fence_timeout_min_value = -1;
static int external_fence_timeout_max_value = -1;
static GucContext external_fence_timeout_context = PGC_INTERNAL;
static int external_fence_timeout_flags = 0;
static char **external_fence_socket_value_addr = NULL;
static const char *external_fence_socket_boot_value = NULL;
static GucContext external_fence_socket_context = PGC_INTERNAL;
static int external_fence_socket_flags = 0;
static GucStringCheckHook external_fence_socket_check_hook = NULL;
#ifdef ENABLE_INJECTION
static char **pcm_x_retain_flush_error_target_value_addr = NULL;
static const char *pcm_x_retain_flush_error_target_boot_value = NULL;
static GucContext pcm_x_retain_flush_error_target_context = PGC_INTERNAL;
static int pcm_x_retain_flush_error_target_flags = 0;
static GucStringCheckHook pcm_x_retain_flush_error_target_check_hook = NULL;
static GucStringAssignHook pcm_x_retain_flush_error_target_assign_hook = NULL;

/* The weak definitions let the pre-implementation binary report a clean RED
 * for the missing injection-only symbols instead of failing at link time.
 * The real cluster_guc object replaces both definitions once D3 lands. */
char *cluster_pcm_x_retain_flush_error_target __attribute__((weak)) = NULL;
extern bool cluster_pcm_x_retain_flush_error_target_matches(uint32 spc_oid,
														 uint32 db_oid,
														 uint32 rel_number,
														 int fork_number,
														 uint32 block_number)
	__attribute__((weak));

bool
cluster_pcm_x_retain_flush_error_target_matches(uint32 spc_oid pg_attribute_unused(),
														 uint32 db_oid pg_attribute_unused(),
														 uint32 rel_number pg_attribute_unused(),
														 int fork_number pg_attribute_unused(),
														 uint32 block_number pg_attribute_unused())
{
	return false;
}
#endif

void
DefineCustomIntVariable(const char *name,
						const char *short_desc pg_attribute_unused(),
						const char *long_desc,
						int *valueAddr, int bootValue,
						int minValue, int maxValue,
						GucContext context, int flags pg_attribute_unused(),
						GucIntCheckHook check_hook pg_attribute_unused(),
						GucIntAssignHook assign_hook pg_attribute_unused(),
						GucShowHook show_hook pg_attribute_unused())
{
	/* Capture the exact R4A capacity contract; real impl lives in PG backend. */
	if (strcmp(name, "cluster.undo_buffers") == 0) {
		undo_buffers_value_addr = valueAddr;
		undo_buffers_boot_value = bootValue;
		undo_buffers_min_value = minValue;
		undo_buffers_max_value = maxValue;
		undo_buffers_context = context;
		undo_buffers_long_desc = long_desc;
	}
	else if (strcmp(name, "cluster.external_fence_acquire_timeout_ms") == 0) {
		external_fence_timeout_value_addr = valueAddr;
		external_fence_timeout_boot_value = bootValue;
		external_fence_timeout_min_value = minValue;
		external_fence_timeout_max_value = maxValue;
		external_fence_timeout_context = context;
		external_fence_timeout_flags = flags;
	}
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
	const char *name, const char *short_desc pg_attribute_unused(),
	const char *long_desc pg_attribute_unused(), char **valueAddr pg_attribute_unused(),
	const char *bootValue, GucContext context,
	int flags, GucStringCheckHook check_hook,
	GucStringAssignHook assign_hook pg_attribute_unused(),
	GucShowHook show_hook pg_attribute_unused())
{
	if (strcmp(name, "cluster.external_fence_socket_path") == 0) {
		external_fence_socket_value_addr = valueAddr;
		external_fence_socket_boot_value = bootValue;
		external_fence_socket_context = context;
		external_fence_socket_flags = flags;
		external_fence_socket_check_hook = check_hook;
	}
#ifdef ENABLE_INJECTION
	else if (strcmp(name, "cluster.pcm_x_retain_flush_error_target") == 0) {
		pcm_x_retain_flush_error_target_value_addr = valueAddr;
		pcm_x_retain_flush_error_target_boot_value = bootValue;
		pcm_x_retain_flush_error_target_context = context;
		pcm_x_retain_flush_error_target_flags = flags;
		pcm_x_retain_flush_error_target_check_hook = check_hook;
		pcm_x_retain_flush_error_target_assign_hook = assign_hook;
	}
#endif
}

static GucBoolCheckHook smart_fusion_check_hook = NULL;
static bool *smart_fusion_value_addr = NULL;
static bool smart_fusion_boot_value = true;
static GucContext smart_fusion_context = PGC_INTERNAL;

void
DefineCustomBoolVariable(const char *name, const char *short_desc pg_attribute_unused(),
						 const char *long_desc pg_attribute_unused(), bool *valueAddr,
						 bool bootValue, GucContext context, int flags pg_attribute_unused(),
						 GucBoolCheckHook check_hook,
						 GucBoolAssignHook assign_hook pg_attribute_unused(),
						 GucShowHook show_hook pg_attribute_unused())
{
	/* Stub for unit-test linking; real impl lives in PG backend.
	 * Added at stage 1.2 for cluster.smgr_user_relations. */
	if (strcmp(name, "cluster.smart_fusion") == 0) {
		smart_fusion_check_hook = check_hook;
		smart_fusion_value_addr = valueAddr;
		smart_fusion_boot_value = bootValue;
		smart_fusion_context = context;
	}
}

/* spec-2.27 D4 stubs — GUC_check_errcode / GUC_check_errdetail macro
 * dependencies (pre_format_elog_string + format_elog_string +
 * GUC_check_errdetail_string global) so cluster_guc.o links standalone
 * even though check_hooks are never invoked in this unit test. */
char *GUC_check_errdetail_string = NULL;
static int last_guc_check_errcode = 0;

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
GUC_check_errcode(int sqlerrcode)
{
	last_guc_check_errcode = sqlerrcode;
}

void *
guc_malloc(int elevel pg_attribute_unused(), size_t size)
{
	return malloc(size);
}

void
pre_format_elog_string(int errnum pg_attribute_unused(), const char *domain pg_attribute_unused())
{}

char *
format_elog_string(const char *fmt, ...)
{
	return (char *)fmt;
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


UT_TEST(test_undo_buffers_guc_describes_both_r4a_banks_and_inactive_zero)
{
	undo_buffers_value_addr = NULL;
	undo_buffers_boot_value = -1;
	undo_buffers_min_value = -1;
	undo_buffers_max_value = -1;
	undo_buffers_context = PGC_INTERNAL;
	undo_buffers_long_desc = NULL;

	cluster_init_guc();

	UT_ASSERT_EQ(undo_buffers_value_addr == &cluster_undo_buffers, true);
	UT_ASSERT_EQ(undo_buffers_boot_value, 2048);
	UT_ASSERT_EQ(undo_buffers_min_value, 0);
	UT_ASSERT_EQ(undo_buffers_max_value, 1048576);
	UT_ASSERT_EQ(undo_buffers_context, PGC_POSTMASTER);
	UT_ASSERT_NOT_NULL(undo_buffers_long_desc);
	if (undo_buffers_long_desc != NULL) {
		UT_ASSERT(strstr(undo_buffers_long_desc, "separate B=D block-zero") != NULL);
		UT_ASSERT(strstr(undo_buffers_long_desc, "20,979,840") != NULL);
		UT_ASSERT(strstr(undo_buffers_long_desc, "R4A is inactive") != NULL);
		UT_ASSERT(strstr(undo_buffers_long_desc, "activation refuses zero") != NULL);
	}
}


UT_TEST(test_smart_fusion_guc_is_guarded_failclosed)
{
	bool newval;
	void *extra = NULL;

	smart_fusion_check_hook = NULL;
	smart_fusion_value_addr = NULL;
	smart_fusion_boot_value = true;
	smart_fusion_context = PGC_INTERNAL;
	GUC_check_errdetail_string = NULL;

	cluster_init_guc();

	UT_ASSERT_NOT_NULL(smart_fusion_check_hook);
	UT_ASSERT_EQ(smart_fusion_value_addr == &cluster_smart_fusion, true);
	UT_ASSERT_EQ(smart_fusion_boot_value, false);
	UT_ASSERT_EQ(smart_fusion_context, PGC_POSTMASTER);

	newval = false;
	extra = NULL;
	UT_ASSERT_EQ(smart_fusion_check_hook(&newval, &extra, PGC_S_TEST), true);
	UT_ASSERT_EQ(cluster_smart_fusion_failclosed_requested(), false);

	newval = true;
	extra = NULL;
	UT_ASSERT_EQ(smart_fusion_check_hook(&newval, &extra, PGC_S_FILE), false);
	UT_ASSERT_EQ(cluster_smart_fusion_failclosed_requested(), true);
	UT_ASSERT_NOT_NULL(GUC_check_errdetail_string);
	if (GUC_check_errdetail_string != NULL)
		UT_ASSERT(strstr(GUC_check_errdetail_string, "cluster.smart_fusion") != NULL);
}


UT_TEST(test_external_fence_guc_contract)
{
	char *valid = "/var/run/pgrac/pgrac-fenced.sock";
	char *relative = "pgrac-fenced.sock";
	char *parent_component = "/var/run/../pgrac-fenced.sock";
	char *dotdot_name = "/var/run/pgrac/..hidden.sock";
	char too_long[sizeof(((struct sockaddr_un *)0)->sun_path) + 2];
	char *too_long_ptr = too_long;
	void *extra = NULL;

	external_fence_timeout_value_addr = NULL;
	external_fence_socket_value_addr = NULL;
	external_fence_socket_check_hook = NULL;
	cluster_init_guc();

	UT_ASSERT_EQ(external_fence_timeout_value_addr ==
				 &cluster_external_fence_acquire_timeout_ms, true);
	UT_ASSERT_EQ(cluster_external_fence_acquire_timeout_ms, 120000);
	UT_ASSERT_EQ(external_fence_timeout_boot_value, 120000);
	UT_ASSERT_EQ(external_fence_timeout_min_value, 1);
	UT_ASSERT_EQ(external_fence_timeout_max_value, 600000);
	UT_ASSERT_EQ(external_fence_timeout_context, PGC_SIGHUP);
	UT_ASSERT_EQ(external_fence_timeout_flags, GUC_UNIT_MS);

	UT_ASSERT_EQ(external_fence_socket_value_addr ==
				 &cluster_external_fence_socket_path, true);
	UT_ASSERT_STR_EQ(external_fence_socket_boot_value,
				 "/var/run/pgrac/pgrac-fenced.sock");
	UT_ASSERT_EQ(external_fence_socket_context, PGC_POSTMASTER);
	UT_ASSERT_EQ(external_fence_socket_flags, 0);
	UT_ASSERT_NOT_NULL(external_fence_socket_check_hook);
	if (external_fence_socket_check_hook != NULL) {
		UT_ASSERT(external_fence_socket_check_hook(&valid, &extra, PGC_S_TEST));
		UT_ASSERT(!external_fence_socket_check_hook(&relative, &extra, PGC_S_TEST));
		UT_ASSERT(!external_fence_socket_check_hook(&parent_component, &extra,
											 PGC_S_TEST));
		UT_ASSERT(external_fence_socket_check_hook(&dotdot_name, &extra,
										 PGC_S_TEST));

		memset(too_long, 'a', sizeof(too_long));
		too_long[0] = '/';
		too_long[sizeof(too_long) - 1] = '\0';
		UT_ASSERT(!external_fence_socket_check_hook(&too_long_ptr, &extra,
											 PGC_S_TEST));
	}
}


#ifdef ENABLE_INJECTION
UT_TEST(test_pcm_x_retain_flush_error_target_guc_contract)
{
	static const char *const invalid_targets[]
		= { "0/5/12345/0/0",
			"1663/0/12345/0/0",
			"1663/5/0/0/0",
			"1663/5/12345/0",
			"1663//12345/0/0",
			"1663/5//0/0",
			"1663/5/12345//0",
			"1663/5/12345/0/",
			"1663/5/12345/0/0/1",
			" 1663/5/12345/0/0",
			"1663/5/12345/0/0 ",
			"1663/ 5/12345/0/0",
			"+1663/5/12345/0/0",
			"-1663/5/12345/0/0",
			"0x67f/5/12345/0/0",
			"1663/5/12x45/0/0",
			"1663/5/12345/0/0suffix",
			"1663/5/12345/0/0\n",
			"4294967296/5/12345/0/0",
			"1663/4294967296/12345/0/0",
			"1663/5/4294967296/0/0",
			"1663/5/12345/4/0",
			"1663/5/12345/0/4294967295" };
	char *value;
	void *extra;
	int i;

	pcm_x_retain_flush_error_target_value_addr = NULL;
	pcm_x_retain_flush_error_target_boot_value = NULL;
	pcm_x_retain_flush_error_target_context = PGC_INTERNAL;
	pcm_x_retain_flush_error_target_flags = 0;
	pcm_x_retain_flush_error_target_check_hook = NULL;
	pcm_x_retain_flush_error_target_assign_hook = NULL;
	cluster_init_guc();

	UT_ASSERT_NOT_NULL(pcm_x_retain_flush_error_target_value_addr);
	UT_ASSERT_NOT_NULL((void *) &cluster_pcm_x_retain_flush_error_target);
	if (&cluster_pcm_x_retain_flush_error_target != NULL)
		UT_ASSERT_EQ(pcm_x_retain_flush_error_target_value_addr ==
						 &cluster_pcm_x_retain_flush_error_target, true);
	UT_ASSERT_STR_EQ(pcm_x_retain_flush_error_target_boot_value, "");
	UT_ASSERT_EQ(pcm_x_retain_flush_error_target_context, PGC_SUSET);
	UT_ASSERT_EQ(pcm_x_retain_flush_error_target_flags, GUC_NOT_IN_SAMPLE);
	UT_ASSERT_NOT_NULL(pcm_x_retain_flush_error_target_check_hook);
	UT_ASSERT_NOT_NULL(pcm_x_retain_flush_error_target_assign_hook);
	UT_ASSERT_NOT_NULL((void *) cluster_pcm_x_retain_flush_error_target_matches);
	if (pcm_x_retain_flush_error_target_check_hook == NULL ||
		pcm_x_retain_flush_error_target_assign_hook == NULL ||
		cluster_pcm_x_retain_flush_error_target_matches == NULL)
		return;

	/* Empty is a valid assignment with a distinct match-none parse result. */
	value = "";
	extra = NULL;
	UT_ASSERT(pcm_x_retain_flush_error_target_check_hook(&value, &extra, PGC_S_TEST));
	UT_ASSERT_NOT_NULL(extra);
	if (extra != NULL) {
		pcm_x_retain_flush_error_target_assign_hook(value, extra);
		free(extra);
	}
	UT_ASSERT(!cluster_pcm_x_retain_flush_error_target_matches(1663, 5, 12345, 0, 0));

	/* Canonical input matches all five fields and rejects each independent
	 * identity mismatch. */
	value = "1663/5/12345/0/0";
	extra = NULL;
	UT_ASSERT(pcm_x_retain_flush_error_target_check_hook(&value, &extra, PGC_S_TEST));
	UT_ASSERT_NOT_NULL(extra);
	if (extra != NULL) {
		pcm_x_retain_flush_error_target_assign_hook(value, extra);
		free(extra);
	}
	UT_ASSERT(cluster_pcm_x_retain_flush_error_target_matches(1663, 5, 12345, 0, 0));
	UT_ASSERT(!cluster_pcm_x_retain_flush_error_target_matches(1664, 5, 12345, 0, 0));
	UT_ASSERT(!cluster_pcm_x_retain_flush_error_target_matches(1663, 6, 12345, 0, 0));
	UT_ASSERT(!cluster_pcm_x_retain_flush_error_target_matches(1663, 5, 12346, 0, 0));
	UT_ASSERT(!cluster_pcm_x_retain_flush_error_target_matches(1663, 5, 12345, 1, 0));
	UT_ASSERT(!cluster_pcm_x_retain_flush_error_target_matches(1663, 5, 12345, 0, 1));

	/* The greatest legal components survive parsing without narrowing. */
	value = "4294967295/4294967295/4294967295/3/4294967294";
	extra = NULL;
	UT_ASSERT(pcm_x_retain_flush_error_target_check_hook(&value, &extra, PGC_S_TEST));
	UT_ASSERT_NOT_NULL(extra);
	if (extra != NULL) {
		pcm_x_retain_flush_error_target_assign_hook(value, extra);
		free(extra);
	}
	UT_ASSERT(cluster_pcm_x_retain_flush_error_target_matches(
		UINT32_MAX, UINT32_MAX, UINT32_MAX, MAX_FORKNUM, InvalidBlockNumber - 1));

	/* Restore the canonical value.  Every rejected check leaves this assigned
	 * parse untouched because the assign hook is never called. */
	value = "1663/5/12345/0/0";
	extra = NULL;
	UT_ASSERT(pcm_x_retain_flush_error_target_check_hook(&value, &extra, PGC_S_TEST));
	if (extra != NULL) {
		pcm_x_retain_flush_error_target_assign_hook(value, extra);
		free(extra);
	}
	for (i = 0; i < lengthof(invalid_targets); i++) {
		value = (char *) invalid_targets[i];
		extra = NULL;
		last_guc_check_errcode = 0;
		UT_ASSERT(!pcm_x_retain_flush_error_target_check_hook(&value, &extra, PGC_S_TEST));
		UT_ASSERT_EQ(last_guc_check_errcode, ERRCODE_INVALID_PARAMETER_VALUE);
		if (extra != NULL)
			free(extra);
		UT_ASSERT(cluster_pcm_x_retain_flush_error_target_matches(
			1663, 5, 12345, 0, 0));
	}
}
#endif


int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_cluster_node_id_default_is_minus_one);
	UT_RUN(test_cluster_node_id_address_stable);
	UT_RUN(test_cluster_init_guc_symbol_is_linkable);
	UT_RUN(test_cluster_phase_remains_plain_global);
	UT_RUN(test_cluster_adg_guc_defaults);
	UT_RUN(test_undo_buffers_guc_describes_both_r4a_banks_and_inactive_zero);
	UT_RUN(test_smart_fusion_guc_is_guarded_failclosed);
	UT_RUN(test_external_fence_guc_contract);
#ifdef ENABLE_INJECTION
	UT_RUN(test_pcm_x_retain_flush_error_target_guc_contract);
#endif
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
