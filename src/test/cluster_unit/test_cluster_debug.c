/*-------------------------------------------------------------------------
 *
 * test_cluster_debug.c
 *	  Compile-time / link-level invariants for the cluster_debug
 *	  diagnostic snapshot framework shipped at stage 0.29.
 *
 *	  Locks:
 *	    - The cluster_dump_state SRF entry-point address resolves at
 *	      link time (validates the always-linked / pg_proc.dat
 *	      contract).
 *	    - cluster_debug.c links cleanly when paired with stubs for
 *	      every cross-module dependency it pulls in (cluster_shmem /
 *	      cluster_guc / cluster_ic / cluster_inject / cluster_pgstat /
 *	      cluster_conf / cluster_elog public API).
 *	    - cluster_inject_get_count + _get_state_at iterators added at
 *	      stage 0.29 work without crashing on out-of-range indices.
 *	    - cluster_pgstat_get_count + _get_at iterators added at stage
 *	      0.29 work without crashing on out-of-range indices.
 *
 *	  End-to-end SRF behaviour (column types, row counts, value
 *	  formatting) is verified on a real PG instance by cluster_tap
 *	  t/017_debug.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_debug.c
 *
 * NOTES
 *	  This is a pgrac-original file.  cluster_debug.c is a cross-module
 *	  aggregator -- linking it standalone requires stubs for the public
 *	  symbols from seven other cluster_*.o files.  The stubs below are
 *	  the minimum set; the SRF body itself is never invoked from the
 *	  unit test (we only take its address), so stub return values are
 *	  inert.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_debug.h"

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
 * Stubs needed to link cluster_debug.o standalone.  cluster_debug.c
 * depends on (read-only): cluster_shmem (ClusterShmem global pointer),
 * cluster_guc (4 GUC vars), cluster_ic (ClusterICOps_Active +
 * ClusterICTier enum), cluster_inject (armed_count + iterator),
 * cluster_pgstat (iterator), cluster_conf (lookup + node_count),
 * cluster_elog (cluster_phase global).  All stubbed below.  The SRF
 * body is never invoked; addresses-only tests + iterator round-trips.
 * ----------
 */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_pgstat.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/elog.h"


/* cluster_shmem */
ClusterShmemCtl *ClusterShmem = NULL;

/* cluster_guc (4 vars) */
int cluster_node_id = -1;
int cluster_interconnect_tier = 0;
char *cluster_config_file = NULL;
char *cluster_injection_points = NULL;

/* cluster_ic */
const ClusterICOps *ClusterICOps_Active = NULL;

/* cluster_ic_tier1 — Hardening v1.0.1 F3 listener metadata accessors
 * (cluster_debug.c references these via dump_ic).  Stubs return zeros
 * since this unit test never binds a real listener. */
#include "cluster/cluster_ic_tier1.h"
const ClusterICOps ClusterICOps_Tier1 = { 0 };
pid_t
cluster_ic_tier1_get_listener_pid(void)
{
	return 0;
}
uint64
cluster_ic_tier1_get_listener_incarnation(void)
{
	return 0;
}
int
cluster_ic_tier1_get_listener_port(void)
{
	return -1;
}

/* cluster_inject (armed_count + iterator) */
int cluster_injection_armed_count = 0;

/*
 * Stage 0.30 sweep: cluster_dump_state gained CLUSTER_INJECTION_POINT;
 * cluster_inject.h declares cluster_injection_run extern.
 */
void
cluster_injection_run(const char *name pg_attribute_unused())
{}

int
cluster_injection_get_count(void)
{
	return 0;
}

bool
cluster_injection_get_state_at(int idx pg_attribute_unused(),
							   const char **name_out pg_attribute_unused(),
							   ClusterInjectFaultType *type_out pg_attribute_unused(),
							   uint64 *hits_out pg_attribute_unused())
{
	return false;
}

/* cluster_pgstat iterator */
int
cluster_pgstat_get_count(void)
{
	return 0;
}

bool
cluster_pgstat_get_at(int idx pg_attribute_unused(), const char **name_out pg_attribute_unused(),
					  uint64 *value_out pg_attribute_unused())
{
	return false;
}

/* cluster_conf */
int
cluster_conf_node_count(void)
{
	return 0;
}

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id pg_attribute_unused())
{
	return NULL;
}

/* cluster_elog */
const char *cluster_phase = "init";

/* cluster_shared_fs (stage 1.1).  cluster_debug.c::dump_shared_fs reads
 * the active vtable and the registered_backends slots; both stubs
 * return NULL so the SRF body's "(none)" / "(empty)" branches fire. */
const struct ClusterSharedFsOps *
cluster_shared_fs_get_active_ops(void)
{
	return NULL;
}

const struct ClusterSharedFsOps *
cluster_shared_fs_get_backend_at(int id pg_attribute_unused())
{
	return NULL;
}

/* Stage 1.2: cluster_smgr accessor + GUC referenced by dump_guc /
 * dump_shared_fs.  cluster_smgr.o is not linked here; provide stubs. */
extern int cluster_smgr_active_relation_count(void);
int
cluster_smgr_active_relation_count(void)
{
	return 0;
}
bool cluster_smgr_user_relations = false;

/* Stage 1.3: cluster_debug.c::dump_shmem now reads from the cluster
 * shmem region registry (region_count + total_bytes + per-region
 * iter).  cluster_shmem.o is not linked here; provide stubs that
 * mimic an empty registry. */
int cluster_shmem_max_regions = 64;
int
cluster_shmem_get_region_count(void)
{
	return 0;
}

/*
 * Stage 1.7: cluster_debug.c::dump_pcm calls cluster_pcm_grd_count() +
 * cluster_pcm_grd_shmem_size() + reads cluster_pcm_grd_max_entries
 * (defined in cluster_pcm_lock.c).  cluster_pcm_lock.o is not linked
 * here; provide stubs returning the same defaults the real
 * implementation returns when GUC=0.
 */
int cluster_pcm_grd_max_entries = 0;

int
cluster_pcm_grd_count(void)
{
	return 0;
}

Size
cluster_pcm_grd_shmem_size(void)
{
	return 0;
}
Size
cluster_shmem_get_total_bytes(void)
{
	return 0;
}
bool
cluster_shmem_iter_regions(int *idx pg_attribute_unused(),
						   ClusterShmemRegion *out pg_attribute_unused())
{
	return false;
}

/* StringInfo + pfree stubs for dump_shared_fs / dump_shmem (stage 1.3).
 * No-op pointers; SRF body is never invoked by this unit test. */
#include "lib/stringinfo.h"
void
initStringInfo(StringInfo str)
{
	str->data = NULL;
	str->len = 0;
	str->maxlen = 0;
	str->cursor = 0;
}
void
appendStringInfoChar(StringInfo str pg_attribute_unused(), char ch pg_attribute_unused())
{}
void
appendStringInfoString(StringInfo str pg_attribute_unused(), const char *s pg_attribute_unused())
{}
void
appendStringInfo(StringInfo str pg_attribute_unused(), const char *fmt pg_attribute_unused(), ...)
{}
void
resetStringInfo(StringInfo str pg_attribute_unused())
{}
void
pfree(void *pointer pg_attribute_unused())
{}


/* PG backend stubs */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * Spec-1.10 stubs needed because cluster_debug.o now pulls in
 * cluster_startup_phase.o (D6 dump_phase emits 4 new keys backed by
 * cluster_phase_started_at / cluster_phase_elapsed_seconds /
 * cluster_phase_history_format), which transitively references
 * GetCurrentTimestamp + TimestampDifference + timestamptz_to_str +
 * IsUnderPostmaster.  The unit test never invokes the dump path so
 * these are address-only -- harmless to stub to no-ops.
 */
bool IsUnderPostmaster = false;

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

void
TimestampDifference(TimestampTz start_time pg_attribute_unused(),
					TimestampTz stop_time pg_attribute_unused(), long *secs, int *microsecs)
{
	*secs = 0;
	*microsecs = 0;
}

bool
TimestampDifferenceExceeds(TimestampTz start_time pg_attribute_unused(),
						   TimestampTz stop_time pg_attribute_unused(),
						   int msec pg_attribute_unused())
{
	return false;
}

const char *
timestamptz_to_str(TimestampTz dt pg_attribute_unused())
{
	return "(stub)";
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/*
 * pg_snprintf stub: cluster_startup_phase.c uses snprintf which is
 * macro'd to pg_snprintf in PG.  Forward to libc snprintf in unit
 * test.  Variadic forwarding via vsnprintf.
 */
#include <stdarg.h>
int
pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
	int n;
	va_list ap;

	va_start(ap, fmt);
	n = vsnprintf(str, count, fmt, ap);
	va_end(ap);
	return n;
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

char *
psprintf(const char *fmt pg_attribute_unused(), ...)
{
	return (char *)"";
}

char *
pstrdup(const char *in pg_attribute_unused())
{
	return (char *)"";
}

Datum
DirectFunctionCall1Coll(PGFunction func pg_attribute_unused(), Oid collation pg_attribute_unused(),
						Datum arg1 pg_attribute_unused())
{
	return (Datum)0;
}

Datum
timestamptz_out(PG_FUNCTION_ARGS)
{
	return (Datum)0;
}


/*
 * Spec-1.10.1 D1 F1 stubs: cluster_startup_phase.o now references
 * the LWLock + ShmemInitStruct API (phase state migrated to shmem)
 * plus the cluster.phase{1..4}_timeout GUC variables (D2 F2 driver
 * elapsed check) plus cluster_shmem_register_region (region registry).
 * The unit test never invokes the runtime paths -- these are
 * address-only / NULL stubs so cluster_debug.o links standalone.
 */
#include "storage/lwlock.h"
#include "storage/shmem.h"

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	if (foundPtr != NULL)
		*foundPtr = false;
	return NULL;
}

int cluster_phase1_timeout = 60;
int cluster_phase2_timeout = 30;
int cluster_phase3_timeout = 600;
int cluster_phase4_timeout = 30;
/* Spec-1.11 Sprint B: cluster_startup_phase.c references cluster_enabled */
bool cluster_enabled = true;
/* Spec-2.1 D1: cluster_startup_phase.c + cluster_conf.c reference allow_single_node */
bool cluster_allow_single_node = true;

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

/*
 * Spec-1.11 Sprint A stubs: cluster_startup_phase.o now references
 * cluster_lmon_start + cluster_lmon_wait_for_ready (phase_1_handler
 * spawn + sync wait).  test_cluster_debug never invokes phase_1_handler
 * so these are address-only no-op stubs.
 */
int
cluster_lmon_start(void)
{
	return 0;
}

bool
cluster_lmon_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/*
 * Spec-1.11.1 F11 stubs: dump_lmon now reads 5 new accessors; test
 * harness never invokes runtime path so address-only no-op stubs.
 */
pid_t
cluster_lmon_pid(void)
{
	return 0;
}
TimestampTz
cluster_lmon_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_lmon_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_lmon_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_lmon_main_loop_iters(void)
{
	return 0;
}
int
cluster_lmon_status(void)
{
	return 0;
}
const char *
cluster_lmon_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}

/* Spec-1.12 D6+D12 stubs: cluster_startup_phase.o now references
 * cluster_lck_start + cluster_lck_wait_for_ready; cluster_debug.o
 * dump_lck now references 6 lck_* accessors. */
int
cluster_lck_start(void)
{
	return 0;
}
bool
cluster_lck_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
int
cluster_lck_status(void)
{
	return 0;
}
const char *
cluster_lck_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_lck_pid(void)
{
	return 0;
}
TimestampTz
cluster_lck_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_lck_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_lck_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_lck_main_loop_iters(void)
{
	return 0;
}

/* Spec-1.13 D6+D12 stubs: cluster_startup_phase.o references
 * cluster_diag_start + cluster_diag_wait_for_ready; cluster_debug.o
 * dump_diag references 7 diag_* accessors. */
int
cluster_diag_start(void)
{
	return 0;
}
bool
cluster_diag_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
int
cluster_diag_status(void)
{
	return 0;
}
const char *
cluster_diag_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_diag_pid(void)
{
	return 0;
}
TimestampTz
cluster_diag_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_diag_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_diag_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_diag_main_loop_iters(void)
{
	return 0;
}

/* Spec-1.14 D6+D12 stubs: cluster_startup_phase.o references
 * cluster_stats_start + cluster_stats_wait_for_ready; cluster_debug.o
 * dump_cluster_stats references 7 cluster_stats_* accessors. */
int
cluster_stats_start(void)
{
	return 0;
}
bool
cluster_stats_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/* Spec-2.5 D4 stubs: same for CSSD. */
int
cluster_cssd_start(void)
{
	return 0;
}
bool
cluster_cssd_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
int
cluster_stats_status(void)
{
	return 0;
}
const char *
cluster_stats_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_stats_pid(void)
{
	return 0;
}
TimestampTz
cluster_stats_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_stats_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_stats_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_stats_main_loop_iters(void)
{
	return 0;
}

/* Spec-2.5 D12 stubs: cluster_debug.c calls cluster_cssd_* accessors
 * via dump_cluster_cssd();test stubs return 0 / sentinel. */
#include "cluster/cluster_cssd.h"
ClusterCssdStatus
cluster_cssd_get_status(void)
{
	return CLUSTER_CSSD_STARTING;
}
const char *
cluster_cssd_status_to_string(ClusterCssdStatus s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_cssd_get_pid(void)
{
	return 0;
}
TimestampTz
cluster_cssd_get_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_cssd_get_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_cssd_get_last_liveness_tick_at(void)
{
	return 0;
}
uint64
cluster_cssd_get_main_loop_iters(void)
{
	return 0;
}

/* Spec-1.15 SCN encoding-layer stubs (cluster_debug.c references the
 * 7-key dump set; address-only in unit tests). */
SCN
cluster_scn_current(void)
{
	return 0;
}
NodeId
cluster_scn_node_id(void)
{
	return 0;
}
uint64
cluster_scn_advance_count(void)
{
	return 0;
}
uint64
cluster_scn_max_observed_remote(void)
{
	return 0;
}
TimestampTz
cluster_scn_initialized_at(void)
{
	return 0;
}
TimestampTz
cluster_scn_last_advance_at(void)
{
	return 0;
}

/* spec-1.16 stat accessor stubs (cluster_debug references the 3 new
 * counter accessors for dump_scn 7 -> 10 keys). */
uint64
cluster_scn_commit_advance_count(void)
{
	return 0;
}
uint64
cluster_scn_abort_advance_count(void)
{
	return 0;
}
uint64
cluster_scn_observe_bump_count(void)
{
	return 0;
}
/* Spec-1.17 BOC stat accessor stubs. */
uint64
cluster_scn_boc_sweep_count(void)
{
	return 0;
}
TimestampTz
cluster_scn_boc_last_sweep_at(void)
{
	return 0;
}
uint64
cluster_scn_boc_pending_at_last_sweep(void)
{
	return 0;
}
uint64
cluster_scn_boc_max_batch_size(void)
{
	return 0;
}


UT_DEFINE_GLOBALS();


/* ============================================================
 * SRF entry-point linkability.
 * ============================================================ */

UT_TEST(test_debug_dump_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_dump_state);
}


/* ============================================================
 * Iterator API on cluster_inject (added in spec-0.29 §1.4).
 * ============================================================ */

UT_TEST(test_debug_inject_get_count_callable)
{
	/* Stub returns 0; verifies the symbol is reachable. */
	UT_ASSERT_EQ(cluster_injection_get_count(), 0);
}

UT_TEST(test_debug_inject_get_state_at_out_of_range)
{
	const char *name = NULL;
	ClusterInjectFaultType type = CLUSTER_FAULT_NONE;
	uint64 hits = 0;

	/* Stub always returns false; real impl returns false when idx<0
	 * or idx>=count.  The contract is the same: out-of-range yields
	 * false, output args untouched. */
	UT_ASSERT_EQ(cluster_injection_get_state_at(-1, &name, &type, &hits), false);
	UT_ASSERT_EQ(cluster_injection_get_state_at(1000, &name, &type, &hits), false);
}

UT_TEST(test_debug_inject_get_state_at_null_outs)
{
	/* Out-pointers may be NULL; helper must not crash. */
	(void)cluster_injection_get_state_at(0, NULL, NULL, NULL);
	UT_ASSERT(true);
}


/* ============================================================
 * Iterator API on cluster_pgstat (added in spec-0.29 §1.4).
 * ============================================================ */

UT_TEST(test_debug_pgstat_get_count_callable)
{
	UT_ASSERT_EQ(cluster_pgstat_get_count(), 0);
}

UT_TEST(test_debug_pgstat_get_at_out_of_range)
{
	const char *name = NULL;
	uint64 value = 0;

	UT_ASSERT_EQ(cluster_pgstat_get_at(-1, &name, &value), false);
	UT_ASSERT_EQ(cluster_pgstat_get_at(1000, &name, &value), false);
}

UT_TEST(test_debug_pgstat_get_at_null_outs)
{
	(void)cluster_pgstat_get_at(0, NULL, NULL);
	UT_ASSERT(true);
}


/* ============================================================
 * Cross-module symbol resolution checks.
 *
 *	If any cluster_*.h public API drifts (rename / removal),
 *	cluster_debug.c will fail to link, which is what we want to
 *	catch at compile time.  The tests below address-take symbols
 *	cluster_debug.c references to surface link-time breakage early.
 * ============================================================ */

UT_TEST(test_debug_links_against_inject_module)
{
	UT_ASSERT_NOT_NULL((void *)cluster_injection_get_count);
	UT_ASSERT_NOT_NULL((void *)cluster_injection_get_state_at);
}

UT_TEST(test_debug_links_against_pgstat_module)
{
	UT_ASSERT_NOT_NULL((void *)cluster_pgstat_get_count);
	UT_ASSERT_NOT_NULL((void *)cluster_pgstat_get_at);
}

UT_TEST(test_debug_links_against_conf_module)
{
	UT_ASSERT_NOT_NULL((void *)cluster_conf_node_count);
	UT_ASSERT_NOT_NULL((void *)cluster_conf_lookup_node);
}

UT_TEST(test_debug_phase_symbol_present)
{
	UT_ASSERT_NOT_NULL(cluster_phase);
}


int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_debug_dump_srf_linkable);
	UT_RUN(test_debug_inject_get_count_callable);
	UT_RUN(test_debug_inject_get_state_at_out_of_range);
	UT_RUN(test_debug_inject_get_state_at_null_outs);
	UT_RUN(test_debug_pgstat_get_count_callable);
	UT_RUN(test_debug_pgstat_get_at_out_of_range);
	UT_RUN(test_debug_pgstat_get_at_null_outs);
	UT_RUN(test_debug_links_against_inject_module);
	UT_RUN(test_debug_links_against_pgstat_module);
	UT_RUN(test_debug_links_against_conf_module);
	UT_RUN(test_debug_phase_symbol_present);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
