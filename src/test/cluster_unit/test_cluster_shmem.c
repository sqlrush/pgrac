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

#include "cluster/cluster_conf.h"
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

/* spec-2.27 D4 stubs — GUC_check_errcode + GUC_check_errdetail macro deps. */
char *GUC_check_errdetail_string = NULL;

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

/* spec-3.18 D2b: cluster_undo_buf check_hook references errdetail. */
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	/* never called when errstart returned false, but stub anyway */
}

/* spec-3.18 D2b: cluster_undo_buf writeback latch reads ClusterConfShmem
 * (cluster_conf_has_peers); NULL => single-node => latch does not block. */
ClusterConf *ClusterConfShmem = NULL;

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

/* spec-3.10 stub: cluster_guc.o (linked here) references the CR cache GUC
 * global; cluster_cr_cache.o is not linked into this test, so stub it. */
int cluster_cr_cache_max_blocks = 64;

/* spec-5.1b D7: cluster_ges_mode_selfcheck GUC removed; cluster_guc.o no
 * longer references it, so the stub definition is gone. */

void
cluster_pcm_lock_module_init(void)
{}

/* Spec-2.32 D2 stub: cluster_init_shmem_module also calls
 * cluster_gcs_module_init (cluster_gcs.c). */
void
cluster_gcs_module_init(void)
{}

/* Spec-2.33 D3 stub: cluster_init_shmem_module also calls
 * cluster_gcs_block_module_init (cluster_gcs_block.c). */
void
cluster_gcs_block_module_init(void)
{}

/* Spec-2.34 D2 stub: cluster_init_shmem_module also calls
 * cluster_gcs_block_dedup_module_init (cluster_gcs_block_dedup.c). */
void
cluster_gcs_block_dedup_module_init(void)
{}

/* Spec-2.38 D2/D3 stub: cluster_init_shmem_module also calls
 * cluster_sinval_module_init (cluster_sinval.c). */
void
cluster_sinval_module_init(void)
{}

/* spec-3.18 D1 stub: cluster_init_shmem_module also calls
 * cluster_undo_buf_register_region (cluster_undo_buf.c, not linked here). */
void
cluster_undo_buf_register_region(void)
{}

/* spec-3.1 D2 + D5 stub: cluster_init_shmem_module also calls
 * cluster_tt_status_shmem_register and cluster_tt_local_shmem_register. */
void
cluster_tt_status_shmem_register(void)
{}
void
cluster_tt_local_shmem_register(void)
{}

/* spec-3.2 D3 stub: cluster_init_shmem_module also calls
 * cluster_tt_status_hint_shmem_register. */
void
cluster_tt_status_hint_shmem_register(void)
{}

/* spec-5.2 D4/D6 stub: cluster_init_shmem_module also calls
 * cluster_tx_enqueue_shmem_register. */
void
cluster_tx_enqueue_shmem_register(void)
{}

/* spec-3.2 D5b stub: cluster_init_shmem_module also calls
 * cluster_visibility_inject_shmem_register. */
void
cluster_visibility_inject_shmem_register(void)
{}

/* spec-3.4b D3 stub: cluster_init_shmem_module also calls
 * cluster_tt_slot_shmem_register. */
void
cluster_tt_slot_shmem_register(void)
{}

/* spec-3.4b D8 stub: cluster_init_shmem additionally calls the Q4 HC
 * activation flush after region iteration. */
void
cluster_tt_status_flush_all_at_activation(void)
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

/* spec-3.13 D1 stub: cluster_init_shmem_module also calls
 * cluster_undo_cleaner_shmem_register; unit harness needs a no-op. */
void
cluster_undo_cleaner_shmem_register(void)
{}

/* spec-5.4 D1 stub: cluster_init_shmem_module also calls
 * cluster_sequence_shmem_register (cluster_sequence_shmem.c); the unit
 * harness does not link that object, so provide a no-op. */
void
cluster_sequence_shmem_register(void)
{}

/* spec-5.5 D8 stub: cluster_init_shmem_module also calls
 * cluster_advisory_shmem_register (cluster_advisory.c); the unit harness does
 * not link that object, so provide a no-op. */
void
cluster_advisory_shmem_register(void)
{}

/* spec-3.4e D6 stub: cluster_init_shmem_module also calls
 * cluster_lock_path_shmem_register (cluster_itl.c).  cluster_unit
 * test binary doesn't link cluster_itl.o;  provide no-op stub. */
void
cluster_lock_path_shmem_register(void)
{}

/* spec-3.5 D5 stub: cluster_init_shmem_module also calls
 * cluster_subtrans_shmem_register (cluster_subtrans.c).  cluster_unit
 * test binary doesn't link cluster_subtrans.o;  provide no-op stub. */
void
cluster_subtrans_shmem_register(void)
{}

/* spec-3.6 D2 stub: cluster_init_shmem_module also calls
 * cluster_multixact_shmem_register (cluster_multixact.c).  cluster_unit
 * test binary doesn't link cluster_multixact.o;  provide no-op stub. */
void
cluster_multixact_shmem_register(void)
{}

/* spec-3.7 D5 stub: cluster_init_shmem_module also calls
 * cluster_undo_record_shmem_register (cluster_undo_record.c).  cluster_unit
 * test binary doesn't link cluster_undo_record.o;  provide no-op stub. */
void
cluster_undo_record_shmem_register(void)
{}

/* spec-3.9 D2 stub: cluster_init_shmem_module also calls
 * cluster_cr_shmem_register (cluster_cr.c).  cluster_unit test binary
 * doesn't link cluster_cr.o;  provide no-op stub. */
void
cluster_cr_shmem_register(void)
{}

/* spec-3.11 D7 stub: cluster_init_shmem_module also calls
 * cluster_tt_durable_shmem_register (cluster_tt_durable_stat.c).  cluster_unit
 * test binary doesn't link that object;  provide no-op stub. */
void
cluster_tt_durable_shmem_register(void)
{}

/* Spec-2.5 Sprint A stub: same for CSSD. */
void
cluster_cssd_shmem_register(void)
{}

/* Spec-1.15 Sprint A stub: same for SCN. */
void
cluster_scn_shmem_register(void)
{}

/* spec-2.13 D8 / L104 stub: cluster_ges shmem region (real impl in
 * cluster_ges.c).  cluster_shmem.c calls cluster_ges_shmem_register
 * to register the "pgrac cluster ges" region; standalone unit test
 * doesn't link cluster_ges.o. */
void
cluster_ges_shmem_register(void)
{}

/* spec-2.23 D1 / L104 stub: cluster_ges_reply_wait shmem region (real
 * impl in cluster_ges_reply_wait.c).  cluster_shmem.c calls
 * cluster_ges_reply_wait_shmem_register to register the "pgrac cluster
 * ges reply wait" region; standalone unit test doesn't link
 * cluster_ges_reply_wait.o. */
void
cluster_ges_reply_wait_shmem_register(void)
{}

/* spec-2.14 D12 / L104 stub: cluster_grd shmem region (real impl in
 * cluster_grd.c).  cluster_shmem.c calls cluster_grd_shmem_register
 * to register the "pgrac cluster grd" region; standalone unit test
 * doesn't link cluster_grd.o. */
void
cluster_grd_shmem_register(void)
{}

/* spec-2.14 D4 / L104 stub: cluster_grd master map init (real impl in
 * cluster_grd.c).  cluster_shmem.c calls cluster_grd_master_map_init
 * after cluster_conf_load (declared-node-aware shard distribution);
 * standalone unit test doesn't link cluster_grd.o. */
void
cluster_grd_master_map_init(void)
{}

/* spec-2.15 D10 / L104 stub: cluster_grd_request_lwlocks (real impl in
 * cluster_grd.c).  cluster_shmem.c::cluster_request_shmem() now invokes
 * this hook once per postmaster init to RequestNamedLWLockTranche; the
 * standalone unit test path must not call PG named-tranche machinery,
 * so the stub is a no-op. */
void
cluster_grd_request_lwlocks(void)
{}

/* spec-2.14 D4 / L104 stub: cluster_conf_node_count (real impl in
 * cluster_conf.c).  cluster_shmem.c calls it as a triple-gate condition
 * before cluster_grd_master_map_init; standalone unit test returns 0 so
 * the gate short-circuits without needing cluster_conf shmem. */
int
cluster_conf_node_count(void)
{
	return 0;
}

/* spec-2.16 D3/D4/D5 L104 stubs:  3 NEW shmem register hooks invoked
 * by cluster_shmem.c::cluster_init_shmem_module().  Vacuous for standalone
 * unit test path. */
void
cluster_grd_pending_shmem_register(void)
{}
void
cluster_grd_outbound_shmem_register(void)
{}
void
cluster_grd_work_queue_shmem_register(void)
{}

/* spec-2.16 cluster_shmem.c also calls RequestNamedLWLockTranche for the
 * 2 NEW Step 2 tranches.  PG built-in stub already declared in this
 * file's "RequestNamedLWLockTranche" extern (from cluster_grd_request_
 * lwlocks block above).  No additional symbol needed. */

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

/* spec-2.6 Sprint A Step 1 stub: cluster_qvotec shmem region. */
void
cluster_qvotec_shmem_register(void)
{}

/* spec-2.28 Sprint A Step 1 stub: cluster_fence shmem region. */
void cluster_fence_shmem_register(void);
void
cluster_fence_shmem_register(void)
{}

/* spec-2.29 Sprint A Step 1 stub: cluster_reconfig shmem region. */
void cluster_reconfig_shmem_register(void);
void
cluster_reconfig_shmem_register(void)
{}

/* spec-4.12 D7 stub: cluster write-fence token region. */
void cluster_write_fence_shmem_register(void);
void
cluster_write_fence_shmem_register(void)
{}

/* spec-2.18 Sprint A Step 1 stub: cluster_lms shmem region (L104). */
void cluster_lms_shmem_register(void);
void
cluster_lms_shmem_register(void)
{}

/* spec-2.19 Sprint A Step 1 stub: cluster_lmd shmem region (L104). */
void cluster_lmd_shmem_register(void);
void
cluster_lmd_shmem_register(void)
{}

/* spec-2.27 D2 stub: cluster_ges_dedup shmem region. */
void cluster_ges_dedup_shmem_register(void);
void cluster_ges_dedup_shmem_request(void);
void
cluster_ges_dedup_shmem_register(void)
{}
void
cluster_ges_dedup_shmem_request(void)
{}

/* spec-4.1 D7 stub: cluster_wal_thread shmem region (L104). */
void cluster_wal_thread_shmem_register(void);
void
cluster_wal_thread_shmem_register(void)
{}

/* spec-4.3 D4 stub: recovery plan shmem region (L104). */
void cluster_recovery_plan_shmem_register(void);
void
cluster_recovery_plan_shmem_register(void)
{}

/* spec-4.10 D6 stub: cluster_init_shmem_module also calls
 * cluster_block_recovery_shmem_register (block-recovery counters). */
void cluster_block_recovery_shmem_register(void);
void
cluster_block_recovery_shmem_register(void)
{}

/* spec-4.5a D10a stubs: per-origin remote-xact SLRU (self-managed,
 * not a registry region; cluster_shmem.c calls all three). */
Size cluster_remote_xact_shmem_size(void);
Size
cluster_remote_xact_shmem_size(void)
{
	return 0;
}
void cluster_remote_xact_shmem_request(void);
void
cluster_remote_xact_shmem_request(void)
{}
void cluster_remote_xact_shmem_init(void);
void
cluster_remote_xact_shmem_init(void)
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
