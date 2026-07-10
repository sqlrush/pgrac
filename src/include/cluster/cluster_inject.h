/*-------------------------------------------------------------------------
 *
 * cluster_inject.h
 *	  pgrac cluster error-injection framework (Stage 0.27).
 *
 *	  Provides named injection points that can be armed at runtime via
 *	  the cluster_inject_fault() SRF or auto-armed at postmaster startup
 *	  via the cluster.injection_points GUC.  When armed, a hit at the
 *	  named site dispatches one of five fault behaviours (ERROR /
 *	  WARNING / SLEEP / CRASH / SKIP) so that "the failure path that
 *	  production never reaches" becomes a regressable code path.
 *
 *	  Design mirrors the PostgreSQL 17 INJECTION_POINT() macro pattern
 *	  but extends it with a fault-type enum + lifetime hit counter
 *	  visible in pg_stat_cluster_injections.  Fast path when no point
 *	  is armed is one atomic read + one branch (~1 ns); the counter
 *	  cluster_injection_armed_count is bumped only by SRF arm/disarm
 *	  so steady-state production paths stay cold.
 *
 *	  See specs/spec-0.27-error-injection.md for stage-0 scope and
 *	  docs/error-injection-design.md for the naming convention,
 *	  fault-type semantics, forbidden insertion sites (critical
 *	  sections / signal handlers), and Stage 1+ injection-point
 *	  roadmap.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_inject.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The CLUSTER_INJECTION_POINT macro and cluster_injection_*
 *	  symbols are available only when configured with --enable-cluster
 *	  (USE_PGRAC_CLUSTER defined).  In disable-cluster builds the
 *	  macro expands to ((void) 0) so call-site insertions cost zero.
 *	  The two SRF entry points (cluster_inject_fault /
 *	  cluster_get_injection_state) are always linked because
 *	  pg_proc.dat references them unconditionally; their bodies
 *	  raise ERRCODE_FEATURE_NOT_SUPPORTED in disable-cluster builds.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_INJECT_H
#define CLUSTER_INJECT_H

#include "fmgr.h"


/* ----------
 * ClusterInjectFaultType -- the five fault behaviours plus NONE.
 *
 *	NONE      counter-only; hits++ but no side effect.  Useful with
 *	          the GUC startup-arm path to verify a code path is hit.
 *	ERROR     hits++ then ereport(ERROR).  Most common for failure-
 *	          path regression.
 *	WARNING   hits++ then ereport(WARNING) and continue.  Default
 *	          fault type for the GUC startup-arm path.
 *	SLEEP     hits++ then pg_usleep(armed_param us) and continue.
 *	          Models slow paths / timeout triggers.
 *	CRASH     hits++ then abort() in cassert builds; degrades to
 *	          ERROR in release builds (matches PG upstream INJECTION_
 *	          POINT semantics, see spec-0.27 §1.4).
 *	SKIP      hits++ then sets a per-backend flag that the caller
 *	          probes with cluster_injection_should_skip(name) to
 *	          decide whether to skip its protected logic.
 * ---------- */
typedef enum ClusterInjectFaultType {
	CLUSTER_FAULT_NONE = 0,
	CLUSTER_FAULT_ERROR = 1,
	CLUSTER_FAULT_WARNING = 2,
	CLUSTER_FAULT_SLEEP = 3,
	CLUSTER_FAULT_CRASH = 4,
	CLUSTER_FAULT_SKIP = 5,
	CLUSTER_FAULT_SKIP_N = 6, /* spec-7.2a: count-based skip (:skipn:N drops N times, then stops) */
} ClusterInjectFaultType;


#ifdef USE_PGRAC_CLUSTER

/* ----------
 * CLUSTER_INJECTION_POINT(name)
 *
 *	The call-site macro.  `name` must be a string literal that matches
 *	an entry in cluster_injection_points[] (see cluster_inject.c).
 *	Fast path when no point is armed: one atomic read + one branch.
 *	Slow path looks up the entry, dispatches the armed fault type,
 *	and increments the lifetime hit counter.
 *
 *	Forbidden insertion sites (see docs/error-injection-design.md §4):
 *	  - inside START_CRIT_SECTION / END_CRIT_SECTION (ERROR PANIC)
 *	  - inside signal handlers (ereport / pg_atomic not safe)
 *	  - per-tuple / per-row hot paths (counter overhead accumulates)
 * ---------- */
#define CLUSTER_INJECTION_POINT(name)                                                              \
	do {                                                                                           \
		if (cluster_injection_armed_count > 0)                                                     \
			cluster_injection_run(name);                                                           \
	} while (0)


/* ----------
 * Symbols backing the macro.  cluster_injection_armed_count is the
 * fast-path gate; cluster_injection_run does the lookup + dispatch.
 * Both are absent in disable-cluster builds (see spec-0.3 contract).
 * ---------- */
extern int cluster_injection_armed_count;

extern void cluster_injection_run(const char *name);

extern bool cluster_injection_should_skip(const char *name);

/*
 * cluster_cr_injection_armed -- spec-3.9 D7 CR-specific armed-state peek.
 *	Returns true iff the named CR injection point is armed; sets *out_param
 *	to its armed_param.  Non-consuming (stays armed until disarmed).  The CR
 *	code raises its own SQLSTATE / runs its own delay from the param.
 */
extern bool cluster_cr_injection_armed(const char *name, uint64 *out_param);

extern void cluster_injection_init_from_guc(void);

/* assign_hook for cluster.injection_points (registered by cluster_init_guc) */
extern void cluster_injection_assign_hook(const char *newval, void *extra);

/* ----------
 * Iterator API (stage 0.29).
 *
 *	Used by cluster_debug.c (pg_cluster_state view) to enumerate every
 *	registered injection point.  cluster_get_injection_state SRF
 *	already exposes the same data via SQL; the iterator below makes
 *	the registry walkable from C call sites without round-tripping
 *	through fmgr.
 * ---------- */
extern int cluster_injection_get_count(void);
extern bool cluster_injection_get_state_at(int idx, const char **name_out,
										   ClusterInjectFaultType *type_out, uint64 *hits_out);

#else /* !USE_PGRAC_CLUSTER */

#define CLUSTER_INJECTION_POINT(name) ((void)0)

#endif /* USE_PGRAC_CLUSTER */


/* ----------
 * SRF entry points.  Always linked (pg_proc.dat references these
 * unconditionally); bodies are #ifdef USE_PGRAC_CLUSTER guarded and
 * raise ERRCODE_FEATURE_NOT_SUPPORTED on disable-cluster builds.
 *
 *	cluster_inject_fault(name text, fault_type text, param int8)
 *	    RETURNS bool
 *	    Arms or disarms the named injection point.  fault_type is one
 *	    of 'none' / 'error' / 'warning' / 'sleep' / 'crash' / 'skip';
 *	    case-insensitive match.  Returns true if the name was found,
 *	    false (with a WARNING) otherwise.  ERRORs unless the caller
 *	    is a superuser.
 *
 *	cluster_get_injection_state()
 *	    RETURNS SETOF (name text, fault_type text, param int8, hits int8)
 *	    Returns one row per registered injection point reflecting the
 *	    current arm state and lifetime hit counter.  Powers the
 *	    pg_stat_cluster_injections view.
 * ---------- */
extern Datum cluster_inject_fault(PG_FUNCTION_ARGS);
extern Datum cluster_get_injection_state(PG_FUNCTION_ARGS);

#endif /* CLUSTER_INJECT_H */
