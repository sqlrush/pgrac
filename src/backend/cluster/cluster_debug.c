/*-------------------------------------------------------------------------
 *
 * cluster_debug.c
 *	  pgrac cluster diagnostic snapshot (Stage 0.29).
 *
 *	  Backs the pg_cluster_state view via the cluster_dump_state SRF.
 *	  Aggregates read-only state from every cluster subsystem into a
 *	  single (category, key, value) result set:
 *
 *	    shmem    ClusterShmem ctl block (magic / version / node_id_at_
 *	             init / created_at)
 *	    guc      cluster.* GUC current values
 *	    ic       active interconnect tier vtable name
 *	    inject   armed_count + per-injection-point fault_type / hits
 *	             (uses cluster_injection_get_count + _get_state_at)
 *	    pgstat   per-counter name / value (uses cluster_pgstat_get_count
 *	             + _get_at)
 *	    conf     pgrac.conf topology summary (node_count + self_in_topology)
 *	    phase    cluster_phase lifecycle string
 *
 *	  Output is ordered by category (fixed registration order) and by
 *	  key inside each category (lexicographic, except injection-point
 *	  children which follow registry order).  See spec-0.29 §3.1 and
 *	  docs/cluster-debug-design.md §2 for the contract.
 *
 *	  Adding a new category at Stage 1+: write a static dump_<name>
 *	  helper following the existing pattern, append a call in
 *	  cluster_dump_state, and update docs/cluster-debug-design.md §3.1
 *	  + matching TAP / unit tests (CLAUDE.md rule 10 three-way sync).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_debug.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  cluster_debug.c is the cross-module aggregator: it reads from
 *	  cluster_shmem / cluster_guc / cluster_ic / cluster_inject /
 *	  cluster_pgstat / cluster_conf / cluster_elog public APIs.  The
 *	  dependency direction is one-way (cluster_debug imports them, not
 *	  the reverse).  No cluster_*.c file should ever include
 *	  cluster_debug.h.
 *
 *	  The SRF entry point is unconditionally compiled because pg_proc.dat
 *	  references it in both build modes; the body is #ifdef USE_PGRAC_
 *	  CLUSTER guarded.  Internal helpers / dumpers are compiled out
 *	  completely on --disable-cluster builds (spec-0.3 contract).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "cluster/cluster_debug.h"
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT (always-linked SRF) */


/* SRF info-V1 declaration -- always linked because pg_proc.dat
 * references this regardless of build mode. */
PG_FUNCTION_INFO_V1(cluster_dump_state);


#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_conf.h"
#include "cluster/cluster_elog.h" /* cluster_phase */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h" /* ClusterICOps_Active, ClusterICTier */
#include "cluster/cluster_pgstat.h"
#include "cluster/cluster_shmem.h"
#include "utils/timestamp.h"


/* ============================================================
 * Row-emission helper.
 *
 *	Every dumper funnels through emit_row to write a single
 *	(category, key, value) triple to the SRF tuplestore.  category
 *	and key are always non-NULL string literals; value may have been
 *	palloc'd by the caller and is consumed by CStringGetTextDatum.
 * ============================================================ */
static void
emit_row(ReturnSetInfo *rsinfo, const char *category, const char *key, const char *value)
{
	Datum values[3];
	bool nulls[3] = { false, false, false };

	values[0] = CStringGetTextDatum(category);
	values[1] = CStringGetTextDatum(key);
	values[2] = CStringGetTextDatum(value ? value : "(null)");

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}


/* ============================================================
 * Value-formatting helpers.
 *
 *	Each returns a palloc'd C string in CurrentMemoryContext (the
 *	tuplestore copies the bytes via CStringGetTextDatum, so the
 *	caller does not need to track lifetime).  Callers must pass the
 *	result through emit_row immediately.
 * ============================================================ */
static char *
fmt_int32(int32 v)
{
	return psprintf("%d", v);
}

static char *
fmt_int64(int64 v)
{
	return psprintf(INT64_FORMAT, v);
}

static char *
fmt_uint32_hex(uint32 v)
{
	return psprintf("0x%08X", v);
}

static char *
fmt_bool(bool v)
{
	return pstrdup(v ? "t" : "f");
}

static char *
fmt_timestamptz(TimestampTz v)
{
	return DatumGetCString(DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(v)));
}

static const char *
str_or_default(const char *s, const char *fallback)
{
	if (s == NULL)
		return fallback;
	if (s[0] == '\0')
		return fallback;
	return s;
}

static const char *
fault_type_to_text(ClusterInjectFaultType t)
{
	switch (t) {
	case CLUSTER_FAULT_NONE:
		return "none";
	case CLUSTER_FAULT_ERROR:
		return "error";
	case CLUSTER_FAULT_WARNING:
		return "warning";
	case CLUSTER_FAULT_SLEEP:
		return "sleep";
	case CLUSTER_FAULT_CRASH:
		return "crash";
	case CLUSTER_FAULT_SKIP:
		return "skip";
	}
	return "unknown";
}

static const char *
ic_tier_to_text(int t)
{
	switch ((ClusterICTier)t) {
	case CLUSTER_IC_TIER_STUB:
		return "stub";
	case CLUSTER_IC_TIER_MOCK:
		return "mock";
	case CLUSTER_IC_TIER_1:
		return "tier1";
	case CLUSTER_IC_TIER_2:
		return "tier2";
	case CLUSTER_IC_TIER_3:
		return "tier3";
	}
	return "unknown";
}


/* ============================================================
 * Per-category dumpers.
 *
 *	Order below matches the registration order in cluster_dump_state.
 *	Stage 1+ subsystems append a new dumper here; see file header for
 *	the procedure.
 * ============================================================ */

static void
dump_shmem(ReturnSetInfo *rsinfo)
{
	if (ClusterShmem == NULL) {
		emit_row(rsinfo, "shmem", "magic", "(null)");
		emit_row(rsinfo, "shmem", "version_packed", "(null)");
		emit_row(rsinfo, "shmem", "node_id_at_init", "(null)");
		emit_row(rsinfo, "shmem", "created_at", "(null)");
		return;
	}

	emit_row(rsinfo, "shmem", "magic", fmt_uint32_hex(ClusterShmem->magic));
	emit_row(rsinfo, "shmem", "version_packed", fmt_uint32_hex(ClusterShmem->version_packed));
	emit_row(rsinfo, "shmem", "node_id_at_init", fmt_int32(ClusterShmem->node_id_at_init));
	emit_row(rsinfo, "shmem", "created_at", fmt_timestamptz(ClusterShmem->created_at));
}

static void
dump_guc(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "guc", "cluster.config_file", str_or_default(cluster_config_file, "(empty)"));
	emit_row(rsinfo, "guc", "cluster.injection_points",
			 str_or_default(cluster_injection_points, "(empty)"));
	emit_row(rsinfo, "guc", "cluster.interconnect_tier",
			 ic_tier_to_text(cluster_interconnect_tier));
	emit_row(rsinfo, "guc", "cluster.node_id", fmt_int32(cluster_node_id));
}

static void
dump_ic(ReturnSetInfo *rsinfo)
{
	const char *tier_name = "(null)";

	if (ClusterICOps_Active != NULL && ClusterICOps_Active->tier_name != NULL)
		tier_name = ClusterICOps_Active->tier_name;

	emit_row(rsinfo, "ic", "active_tier_name", tier_name);
}

static void
dump_inject(ReturnSetInfo *rsinfo)
{
	int n;

	emit_row(rsinfo, "inject", "armed_count", fmt_int32(cluster_injection_armed_count));

	n = cluster_injection_get_count();
	for (int i = 0; i < n; i++) {
		const char *name = NULL;
		ClusterInjectFaultType type = CLUSTER_FAULT_NONE;
		uint64 hits = 0;
		char *key_type;
		char *key_hits;

		if (!cluster_injection_get_state_at(i, &name, &type, &hits))
			continue;
		if (name == NULL)
			continue;

		key_type = psprintf("%s.fault_type", name);
		key_hits = psprintf("%s.hits", name);
		emit_row(rsinfo, "inject", key_type, fault_type_to_text(type));
		emit_row(rsinfo, "inject", key_hits, fmt_int64((int64)hits));
	}
}

static void
dump_pgstat(ReturnSetInfo *rsinfo)
{
	int n = cluster_pgstat_get_count();

	for (int i = 0; i < n; i++) {
		const char *name = NULL;
		uint64 value = 0;

		if (!cluster_pgstat_get_at(i, &name, &value))
			continue;
		if (name == NULL)
			continue;

		emit_row(rsinfo, "pgstat", name, fmt_int64((int64)value));
	}
}

static void
dump_conf(ReturnSetInfo *rsinfo)
{
	int node_count = cluster_conf_node_count();
	bool self_in_topology = false;

	if (cluster_node_id >= 0)
		self_in_topology = (cluster_conf_lookup_node(cluster_node_id) != NULL);

	emit_row(rsinfo, "conf", "node_count", fmt_int32(node_count));
	emit_row(rsinfo, "conf", "self_in_topology", fmt_bool(self_in_topology));
}

static void
dump_phase(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "phase", "cluster_phase", str_or_default(cluster_phase, "(unset)"));
}

#endif /* USE_PGRAC_CLUSTER */


/* ============================================================
 * SRF entry point (always linked; body guarded by USE_PGRAC_CLUSTER).
 * ============================================================ */

Datum
cluster_dump_state(PG_FUNCTION_ARGS)
{
	CLUSTER_INJECTION_POINT("cluster-debug-dump-entry");

	InitMaterializedSRF(fcinfo, 0);

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

		/*
		 * Fixed category order (spec-0.29 §3.1).  Each helper emits
		 * its own keys in the order documented in
		 * docs/cluster-debug-design.md §3.
		 */
		dump_shmem(rsinfo);
		dump_guc(rsinfo);
		dump_ic(rsinfo);
		dump_inject(rsinfo);
		dump_pgstat(rsinfo);
		dump_conf(rsinfo);
		dump_phase(rsinfo);
	}
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_state requires --enable-cluster")));
#endif

	return (Datum)0;
}
