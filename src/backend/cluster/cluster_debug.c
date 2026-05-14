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
#include "cluster/cluster_diag.h" /* cluster_diag_status (spec-1.13 D12) */
#include "cluster/cluster_lck.h"  /* cluster_lck_status (spec-1.12 D12) */
#include "cluster/cluster_scn.h"  /* cluster_scn_current (spec-1.15 D6) */
#include "cluster/cluster_ges.h"  /* cluster_ges_{request,reply}_defer_count (spec-2.13 D4) */
#include "cluster/cluster_grd.h"  /* cluster_grd_* observability accessors (spec-2.14 D6) */
#include "cluster/cluster_lms.h"  /* cluster_lms_* observability accessors (spec-2.18 D10) */
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_grd_pending.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_cssd.h"  /* cluster_cssd_status (spec-2.5 D12) */
#include "cluster/cluster_stats.h" /* cluster_stats_status (spec-1.14 D12) */
#include "cluster/cluster_lmon.h"  /* cluster_lmon_status (spec-1.11 Sprint B D12) */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"			   /* ClusterICOps_Active, ClusterICTier */
#include "cluster/cluster_ic_tier1.h"	   /* listener metadata accessors (Hardening v1.0.1 F3) */
#include "cluster/cluster_scn.h"		   /* SCN typedef (stage 1.4) */
#include "cluster/cluster_itl_slot.h"	   /* CLUSTER_ITL_* constants (stage 1.5) */
#include "cluster/cluster_buffer_desc.h"   /* BufferType / PcmState enums (stage 1.6) */
#include "cluster/cluster_pcm_lock.h"	   /* PCM stub API + grd helpers (stage 1.7) */
#include "cluster/cluster_startup_phase.h" /* phase enum + accessors (stage 1.10) */
#include "storage/bufpage.h"	   /* PG_PAGE_LAYOUT_VERSION, SizeOfPageHeaderData (stage 1.4) */
#include "storage/buf_internals.h" /* BufferDesc layout (stage 1.6) */
#include "cluster/cluster_pgstat.h"
#include "cluster/cluster_shmem.h"
#include "cluster/storage/cluster_shared_fs.h" /* dump_shared_fs (stage 1.1) */
#include "cluster/storage/cluster_smgr.h"	   /* cluster_smgr_active_relation_count (stage 1.2) */
#include "lib/stringinfo.h"
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

/* Hardening v1.0.1 (round 8 P2): full 64-bit hex formatter for SCN
 * and other 64-bit identifiers.  scn_current_encoded was previously
 * truncated to the high 32 bits, hiding the entire local_scn (low 56
 * bits).  All future cluster shmem 64-bit fields should use this. */
static char *
fmt_uint64_hex(uint64 v)
{
	return psprintf("0x%016" INT64_MODIFIER "X", v);
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
	int idx;
	ClusterShmemRegion region;
	StringInfoData key_buf;

	if (ClusterShmem == NULL) {
		emit_row(rsinfo, "shmem", "magic", "(null)");
		emit_row(rsinfo, "shmem", "version_packed", "(null)");
		emit_row(rsinfo, "shmem", "node_id_at_init", "(null)");
		emit_row(rsinfo, "shmem", "created_at", "(null)");
	} else {
		emit_row(rsinfo, "shmem", "magic", fmt_uint32_hex(ClusterShmem->magic));
		emit_row(rsinfo, "shmem", "version_packed", fmt_uint32_hex(ClusterShmem->version_packed));
		emit_row(rsinfo, "shmem", "node_id_at_init", fmt_int32(ClusterShmem->node_id_at_init));
		emit_row(rsinfo, "shmem", "created_at", fmt_timestamptz(ClusterShmem->created_at));
	}

	/*
	 * Stage 1.3: per-region rollup from the cluster shmem registry.
	 * region_count + total_bytes are the summary; region.<name>.bytes
	 * + region.<name>.owner expand each registered region for direct
	 * lookup.  Both surfaces complement pg_cluster_shmem (the SQL view)
	 * which is the structured per-row source of truth.
	 */
	emit_row(rsinfo, "shmem", "region_count", fmt_int32(cluster_shmem_get_region_count()));
	emit_row(rsinfo, "shmem", "total_bytes", fmt_int64((int64)cluster_shmem_get_total_bytes()));

	initStringInfo(&key_buf);
	idx = 0;
	while (cluster_shmem_iter_regions(&idx, &region)) {
		resetStringInfo(&key_buf);
		appendStringInfo(&key_buf, "region.%s.bytes", region.name);
		emit_row(rsinfo, "shmem", key_buf.data, fmt_int64((int64)region.size_fn()));

		resetStringInfo(&key_buf);
		appendStringInfo(&key_buf, "region.%s.owner", region.name);
		emit_row(rsinfo, "shmem", key_buf.data, region.owner_subsys);
	}
	pfree(key_buf.data);
}

static void
dump_guc(ReturnSetInfo *rsinfo)
{
	const ClusterSharedFsOps *shared_fs_active;

	emit_row(rsinfo, "guc", "cluster.config_file", str_or_default(cluster_config_file, "(empty)"));
	emit_row(rsinfo, "guc", "cluster.injection_points",
			 str_or_default(cluster_injection_points, "(empty)"));
	emit_row(rsinfo, "guc", "cluster.interconnect_tier",
			 ic_tier_to_text(cluster_interconnect_tier));
	emit_row(rsinfo, "guc", "cluster.node_id", fmt_int32(cluster_node_id));

	/*
	 * Stage 1.1: cluster.shared_storage_backend value as a human-readable
	 * backend name (looked up from the active vtable rather than mapping
	 * the int again here).  Pre-init backends fall back to "(none)" so
	 * the row remains present for diagnostic stability.
	 */
	shared_fs_active = cluster_shared_fs_get_active_ops();
	emit_row(rsinfo, "guc", "cluster.shared_storage_backend",
			 shared_fs_active != NULL ? shared_fs_active->name : "(none)");

	/* Stage 1.2: cluster.smgr_user_relations boolean. */
	emit_row(rsinfo, "guc", "cluster.smgr_user_relations", fmt_bool(cluster_smgr_user_relations));

	/* Stage 1.3: cluster.shmem_max_regions int. */
	emit_row(rsinfo, "guc", "cluster.shmem_max_regions", fmt_int32(cluster_shmem_max_regions));
}

static void
dump_ic(ReturnSetInfo *rsinfo)
{
	const char *tier_name = "(null)";

	if (ClusterICOps_Active != NULL && ClusterICOps_Active->tier_name != NULL)
		tier_name = ClusterICOps_Active->tier_name;

	emit_row(rsinfo, "ic", "active_tier_name", tier_name);

	/*
	 * Hardening v1.0.1 F3: expose listener metadata so observers can
	 * detect "LMON has respawned, listener was rebound".  Useful for
	 * t/077 TAP and runtime diagnostics; the fd itself is process-
	 * local and never exposed.
	 */
	if (ClusterICOps_Active == &ClusterICOps_Tier1) {
		emit_row(rsinfo, "ic", "tier1_listener_pid",
				 fmt_int32((int32)cluster_ic_tier1_get_listener_pid()));
		emit_row(rsinfo, "ic", "tier1_listener_incarnation",
				 psprintf(UINT64_FORMAT, cluster_ic_tier1_get_listener_incarnation()));
		emit_row(rsinfo, "ic", "tier1_listener_port",
				 fmt_int32((int32)cluster_ic_tier1_get_listener_port()));
	}
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
	ClusterStartupPhase current = cluster_current_phase();
	TimestampTz started = cluster_phase_started_at(current);
	char history_buf[1024];

	/*
	 * Spec-1.10.2 F7 (2026-05-04 codex review fix): the SQL-visible
	 * "cluster_phase" key MUST derive from shmem-backed
	 * cluster_current_phase() instead of the legacy const char *
	 * cluster_phase global.  The legacy mirror is fork-coherent (child
	 * inherits postmaster's last write) but EXEC_BACKEND children
	 * re-exec and re-run the static initializer -> the mirror reverts
	 * to "pre_init" while shmem still holds the live phase.  Reading
	 * via cluster_startup_phase_to_string(current) closes that gap.
	 */
	emit_row(rsinfo, "phase", "cluster_phase", cluster_startup_phase_to_string(current));

	/*
	 * Spec-1.10 (2026-05-03) phase 4 new keys (HC5 fixed-size ring on
	 * phase_history; user 修订 5).
	 */
	emit_row(rsinfo, "phase", "phase_enum_value", fmt_int32((int32)current));

	if (started == 0) {
		emit_row(rsinfo, "phase", "phase_started_at", "(unset)");
		emit_row(rsinfo, "phase", "phase_elapsed_seconds", fmt_int64(0));
	} else {
		emit_row(rsinfo, "phase", "phase_started_at", pstrdup(timestamptz_to_str(started)));
		emit_row(rsinfo, "phase", "phase_elapsed_seconds",
				 fmt_int64(cluster_phase_elapsed_seconds()));
	}

	cluster_phase_history_format(history_buf, sizeof(history_buf));
	emit_row(rsinfo, "phase", "phase_history",
			 pstrdup(history_buf[0] != '\0' ? history_buf : "(empty)"));
}


/*
 * dump_lmon -- Stage 1.11 Sprint B LMON state diagnostics
 * (spec-1.11 D12).  Six SQL keys exposed to pg_cluster_state.lmon
 * for operators to monitor LMON liveness without log-grepping.  All
 * reads go through cluster_lmon_status() / cluster_lmon_state shmem
 * (HC2 SSOT, HC3 limited scope).
 */
static void
dump_lmon(ReturnSetInfo *rsinfo)
{
	ClusterLmonStatus s = cluster_lmon_status();
	pid_t pid;
	TimestampTz spawned_at, ready_at, last_tick;
	int64 iters;

	emit_row(rsinfo, "lmon", "lmon_status", cluster_lmon_status_to_string(s));
	emit_row(rsinfo, "lmon", "lmon_status_enum_value", fmt_int32((int32)s));

	/*
	 * Spec-1.11.1 F11 (codex round 4 P2 fix): emit the 5 keys Sprint B
	 * D12 left out so cluster.lmon_main_loop_interval GUC + LMON
	 * liveness are SQL-verifiable.  pid==0 / timestamps==0 surface as
	 * "(unset)" to match other lifecycle keys; main_loop_iters is
	 * always int8.
	 */
	pid = cluster_lmon_pid();
	emit_row(rsinfo, "lmon", "lmon_pid", pid == 0 ? "(unset)" : fmt_int64((int64)pid));

	spawned_at = cluster_lmon_spawned_at();
	emit_row(rsinfo, "lmon", "lmon_spawned_at",
			 spawned_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(spawned_at)));

	ready_at = cluster_lmon_ready_at();
	emit_row(rsinfo, "lmon", "lmon_ready_at",
			 ready_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(ready_at)));

	last_tick = cluster_lmon_last_liveness_tick_at();
	emit_row(rsinfo, "lmon", "lmon_last_liveness_tick_at",
			 last_tick == 0 ? "(unset)" : pstrdup(timestamptz_to_str(last_tick)));

	iters = cluster_lmon_main_loop_iters();
	emit_row(rsinfo, "lmon", "lmon_main_loop_iters", fmt_int64(iters));
}

/*
 * dump_lck -- Stage 1.12 LCK state diagnostics (mirrors dump_lmon
 * spec-1.11.1 F11 6 keys complete model).  Sprint A starts with full
 * 6 keys, not the Sprint B starter trap that bit spec-1.11 D12.
 */
static void
dump_lck(ReturnSetInfo *rsinfo)
{
	ClusterLckStatus s = cluster_lck_status();
	pid_t pid;
	TimestampTz spawned_at, ready_at, last_tick;
	int64 iters;

	emit_row(rsinfo, "lck", "lck_status", cluster_lck_status_to_string(s));
	emit_row(rsinfo, "lck", "lck_status_enum_value", fmt_int32((int32)s));

	pid = cluster_lck_pid();
	emit_row(rsinfo, "lck", "lck_pid", pid == 0 ? "(unset)" : fmt_int64((int64)pid));

	spawned_at = cluster_lck_spawned_at();
	emit_row(rsinfo, "lck", "lck_spawned_at",
			 spawned_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(spawned_at)));

	ready_at = cluster_lck_ready_at();
	emit_row(rsinfo, "lck", "lck_ready_at",
			 ready_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(ready_at)));

	last_tick = cluster_lck_last_liveness_tick_at();
	emit_row(rsinfo, "lck", "lck_last_liveness_tick_at",
			 last_tick == 0 ? "(unset)" : pstrdup(timestamptz_to_str(last_tick)));

	iters = cluster_lck_main_loop_iters();
	emit_row(rsinfo, "lck", "lck_main_loop_iters", fmt_int64(iters));
}


/*
 * dump_diag -- Stage 1.13 DIAG state diagnostics (mirrors dump_lck /
 * dump_lmon F11 7-key complete model: 2 status + 5 lifecycle).
 */
static void
dump_diag(ReturnSetInfo *rsinfo)
{
	ClusterDiagStatus s = cluster_diag_status();
	pid_t pid;
	TimestampTz spawned_at, ready_at, last_tick;
	int64 iters;

	emit_row(rsinfo, "diag", "diag_status", cluster_diag_status_to_string(s));
	emit_row(rsinfo, "diag", "diag_status_enum_value", fmt_int32((int32)s));

	pid = cluster_diag_pid();
	emit_row(rsinfo, "diag", "diag_pid", pid == 0 ? "(unset)" : fmt_int64((int64)pid));

	spawned_at = cluster_diag_spawned_at();
	emit_row(rsinfo, "diag", "diag_spawned_at",
			 spawned_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(spawned_at)));

	ready_at = cluster_diag_ready_at();
	emit_row(rsinfo, "diag", "diag_ready_at",
			 ready_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(ready_at)));

	last_tick = cluster_diag_last_liveness_tick_at();
	emit_row(rsinfo, "diag", "diag_last_liveness_tick_at",
			 last_tick == 0 ? "(unset)" : pstrdup(timestamptz_to_str(last_tick)));

	iters = cluster_diag_main_loop_iters();
	emit_row(rsinfo, "diag", "diag_main_loop_iters", fmt_int64(iters));
}


/*
 * dump_cluster_stats -- Stage 1.14 Cluster Stats state diagnostics
 * (mirrors dump_diag F11 7-key complete model: 2 status + 5 lifecycle).
 */
static void
dump_cluster_stats(ReturnSetInfo *rsinfo)
{
	ClusterStatsStatus s = cluster_stats_status();
	pid_t pid;
	TimestampTz spawned_at, ready_at, last_tick;
	int64 iters;

	emit_row(rsinfo, "cluster_stats", "cluster_stats_status", cluster_stats_status_to_string(s));
	emit_row(rsinfo, "cluster_stats", "cluster_stats_status_enum_value", fmt_int32((int32)s));

	pid = cluster_stats_pid();
	emit_row(rsinfo, "cluster_stats", "cluster_stats_pid",
			 pid == 0 ? "(unset)" : fmt_int64((int64)pid));

	spawned_at = cluster_stats_spawned_at();
	emit_row(rsinfo, "cluster_stats", "cluster_stats_spawned_at",
			 spawned_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(spawned_at)));

	ready_at = cluster_stats_ready_at();
	emit_row(rsinfo, "cluster_stats", "cluster_stats_ready_at",
			 ready_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(ready_at)));

	last_tick = cluster_stats_last_liveness_tick_at();
	emit_row(rsinfo, "cluster_stats", "cluster_stats_last_liveness_tick_at",
			 last_tick == 0 ? "(unset)" : pstrdup(timestamptz_to_str(last_tick)));

	iters = cluster_stats_main_loop_iters();
	emit_row(rsinfo, "cluster_stats", "cluster_stats_main_loop_iters", fmt_int64(iters));
}


/*
 * dump_cluster_cssd -- Stage 2.5 CSSD aux process state diagnostics
 * (mirrors dump_cluster_stats F11 7-key complete model: 2 status + 5
 * lifecycle).
 */
static void
dump_cluster_cssd(ReturnSetInfo *rsinfo)
{
	ClusterCssdStatus s = cluster_cssd_get_status();
	pid_t pid;
	TimestampTz spawned_at, ready_at, last_tick;
	uint64 iters;

	emit_row(rsinfo, "cluster_cssd", "cluster_cssd_status", cluster_cssd_status_to_string(s));
	emit_row(rsinfo, "cluster_cssd", "cluster_cssd_status_enum_value", fmt_int32((int32)s));

	pid = cluster_cssd_get_pid();
	emit_row(rsinfo, "cluster_cssd", "cluster_cssd_pid",
			 pid == 0 ? "(unset)" : fmt_int64((int64)pid));

	spawned_at = cluster_cssd_get_spawned_at();
	emit_row(rsinfo, "cluster_cssd", "cluster_cssd_spawned_at",
			 spawned_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(spawned_at)));

	ready_at = cluster_cssd_get_ready_at();
	emit_row(rsinfo, "cluster_cssd", "cluster_cssd_ready_at",
			 ready_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(ready_at)));

	last_tick = cluster_cssd_get_last_liveness_tick_at();
	emit_row(rsinfo, "cluster_cssd", "cluster_cssd_last_liveness_tick_at",
			 last_tick == 0 ? "(unset)" : pstrdup(timestamptz_to_str(last_tick)));

	iters = cluster_cssd_get_main_loop_iters();
	emit_row(rsinfo, "cluster_cssd", "cluster_cssd_main_loop_iters", fmt_int64((int64)iters));

	/*
	 * spec-2.5 Hardening v1.0.3:  declared-alive aggregate observability
	 * substrate.  Pure observability — these keys MUST NOT be consumed
	 * for any decision path (quorum_state / reconfig / fence broadcast).
	 * Provided as SQL surface for future fence/reconfig/SCN consumers
	 * to verify substrate health from operator perspective.
	 */
	{
		int alive_count = cluster_cssd_get_declared_alive_count();
		uint8 alive_bitmap[CLUSTER_CSSD_PEER_ALIVE_BITMAP_BYTES];
		char hex_buf[2 + CLUSTER_CSSD_PEER_ALIVE_BITMAP_BYTES * 2 + 1];
		int i;

		emit_row(rsinfo, "cluster_cssd", "cssd.declared_alive_count",
				 fmt_int32((int32)alive_count));

		cluster_cssd_get_declared_alive_bitmap(alive_bitmap);
		hex_buf[0] = '0';
		hex_buf[1] = 'x';
		for (i = 0; i < CLUSTER_CSSD_PEER_ALIVE_BITMAP_BYTES; i++)
			snprintf(hex_buf + 2 + (i * 2), 3, "%02x", alive_bitmap[i]);
		hex_buf[2 + CLUSTER_CSSD_PEER_ALIVE_BITMAP_BYTES * 2] = '\0';
		emit_row(rsinfo, "cluster_cssd", "cssd.declared_alive_bitmap", pstrdup(hex_buf));
	}
}


/*
 * dump_scn -- Stage 1.15 SCN encoding-layer state diagnostics.
 *
 *	7 keys: scn_node_id / scn_current_local / scn_current_encoded /
 *	scn_max_observed_remote / scn_total_advance_count /
 *	scn_initialized_at / scn_last_advance_at.
 *
 *	Spec-1.15 Q6: 7 keys 起步即完整, mirrors lmon/lck/diag/stats dump
 *	7-key model.  scn_current_encoded uses hex format for clarity
 *	(8-bit node_id high byte + 56-bit local_scn) per docs/scn-protocol-
 *	design.md §3.1.
 */
static void
dump_scn(ReturnSetInfo *rsinfo)
{
	NodeId node_id;
	SCN current;
	uint64 current_local;
	uint64 max_remote;
	uint64 advance_count;
	TimestampTz init_at;
	TimestampTz last_at;

	node_id = cluster_scn_node_id();
	current = cluster_scn_current();
	current_local = scn_local(current);
	max_remote = cluster_scn_max_observed_remote();
	advance_count = cluster_scn_advance_count();
	init_at = cluster_scn_initialized_at();
	last_at = cluster_scn_last_advance_at();

	emit_row(rsinfo, "scn", "scn_node_id", fmt_int32((int32)node_id));
	emit_row(rsinfo, "scn", "scn_current_local", fmt_int64((int64)current_local));
	/* Hardening v1.0.1 (round 8 P2): full 64-bit hex.  Previously
	 * truncated to the high 32 bits, which collapsed every (node, local)
	 * pair sharing the same node into one displayed value -- node=7,
	 * local=1 and node=7, local=999 both showed 0x07000000.  Reading
	 * the full 64-bit pattern is the documented "encoded SCN". */
	emit_row(rsinfo, "scn", "scn_current_encoded", fmt_uint64_hex((uint64)current));
	emit_row(rsinfo, "scn", "scn_max_observed_remote", fmt_int64((int64)max_remote));
	emit_row(rsinfo, "scn", "scn_total_advance_count", fmt_int64((int64)advance_count));
	emit_row(rsinfo, "scn", "scn_initialized_at",
			 init_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(init_at)));
	emit_row(rsinfo, "scn", "scn_last_advance_at",
			 last_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(last_at)));
	/* spec-1.16 D6: per-decision counters (Q6 dump_scn 7 -> 10 keys) */
	emit_row(rsinfo, "scn", "scn_commit_advance_count",
			 fmt_int64((int64)cluster_scn_commit_advance_count()));
	emit_row(rsinfo, "scn", "scn_abort_advance_count",
			 fmt_int64((int64)cluster_scn_abort_advance_count()));
	emit_row(rsinfo, "scn", "scn_observe_bump_count",
			 fmt_int64((int64)cluster_scn_observe_bump_count()));
	/* spec-1.17 D6 (Q5 dump_scn 10 -> 14 keys): BOC sweep stats.
	 * scn_last_advance_at semantics changed in spec-1.17: now BOC
	 * approximation (refreshed at sweep, ≤ boc_sweep_interval_ms
	 * staleness vs spec-1.16 per-commit refresh). */
	{
		TimestampTz boc_at = cluster_scn_boc_last_sweep_at();
		emit_row(rsinfo, "scn", "scn_boc_sweep_count",
				 fmt_int64((int64)cluster_scn_boc_sweep_count()));
		emit_row(rsinfo, "scn", "scn_boc_last_sweep_at",
				 boc_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(boc_at)));
		emit_row(rsinfo, "scn", "scn_boc_pending_at_last_sweep",
				 fmt_int64((int64)cluster_scn_boc_pending_at_last_sweep()));
		emit_row(rsinfo, "scn", "scn_boc_max_batch_size",
				 fmt_int64((int64)cluster_scn_boc_max_batch_size()));
		/* PGRAC: spec-2.10 D5 — LMON drain-side success-batch counter.
		 * Pairs with scn_boc_sweep_count for producer/consumer view.
		 * Diff主要反映 LMON coalescing,见 spec-2.10 §2.2 / §3.0 I3. */
		emit_row(rsinfo, "scn", "scn_boc_broadcast_fanout_count",
				 fmt_int64((int64)cluster_scn_boc_broadcast_fanout_count()));
		/* PGRAC: spec-2.11 D5 — cross-instance commit_scn lookup defer
		 * counter.  Skeleton-only;  stub always returns DEFER and bumps
		 * this counter.  See spec-2.11 §2.2 + §3.0 I1. */
		emit_row(rsinfo, "scn", "scn_commit_lookup_defer_count",
				 fmt_int64((int64)cluster_scn_commit_lookup_defer_count()));

		/* PGRAC: spec-2.12 D5 — SCN convergence boundary verification
		 * metric (3 rows):  last_observe_at + seconds_since_last_observe
		 * (derived) + observed_max_observe_gap_ms.  See spec-2.12 §2.5. */
		{
			TimestampTz last_obs = cluster_scn_last_observe_at();

			emit_row(rsinfo, "scn", "scn_last_observe_at",
					 last_obs == 0 ? "(unset)" : pstrdup(timestamptz_to_str(last_obs)));

			if (last_obs == 0) {
				emit_row(rsinfo, "scn", "scn_seconds_since_last_observe", "(unset)");
			} else {
				TimestampTz now_ts = GetCurrentTimestamp();
				double seconds;
				char buf[32];

				/* Wall-clock steps can move GetCurrentTimestamp() behind a
				 * previously recorded observe timestamp.  This is an
				 * observability row, so clamp to zero rather than exposing a
				 * negative "seconds since" value. */
				seconds = now_ts > last_obs ? (now_ts - last_obs) / 1000000.0 : 0.0;
				snprintf(buf, sizeof(buf), "%.3f", seconds);
				emit_row(rsinfo, "scn", "scn_seconds_since_last_observe", pstrdup(buf));
			}

			emit_row(rsinfo, "scn", "scn_observed_max_observe_gap_ms",
					 fmt_int64((int64)cluster_scn_observed_max_observe_gap_ms()));
		}
	}
}


/*
 * dump_grd -- spec-2.14 D6 GRD routing substrate observability.
 *
 *	Emits 14 rows under category='grd' (8 from spec-2.14 + 6 from
 *	spec-2.15 entry-table infrastructure):
 *	  - grd_shard_count:             4096 (constant)
 *	  - grd_local_master_count:      shards mastered by self node
 *	  - grd_remote_master_count:     4096 - local (SQL-friendly though derivable)
 *	  - grd_shard_lookup_count:      total lookup invocations (v0.4 NEW)
 *	  - grd_local_master_lookup_count:  lookup_master() == self count
 *	  - grd_remote_master_lookup_count: lookup_master() != self count
 *	  - grd_resid_encode_count:      resid_encode invocations (v0.4 NEW)
 *	  - grd_master_map_refresh_count: init + future DRM refresh count
 *	  - grd_max_entries:             cluster.grd_max_entries GUC value
 *	  - grd_entry_count:             current live entry count
 *	  - grd_allocated_bytes:         entry HTAB allocation estimate
 *	  - grd_entry_create_count:      lifetime created entries
 *	  - grd_entry_lookup_hit_count:  lifetime OK lookups
 *	  - grd_entry_full_count:        lifetime FULL returns
 *
 *	Counter invariant (v0.4 P1.2):
 *	  grd_shard_lookup_count >=
 *	      grd_local_master_lookup_count + grd_remote_master_lookup_count
 *	  (>= not =;  shard_lookup() thin wrapper increments total only)
 *
 *	Substrate phase (spec-2.14 ship):  no caller-side LockAcquire integration
 *	(spec-2.15+),  so counters stay 0 in production until spec-2.15+ wires
 *	real callers.  Future spec-2.15 entry table operations split counters
 *	by GES state (GRANTED / WAITING / CONVERTING / DEADLOCK).
 */
static void
dump_grd(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "grd", "grd_shard_count", fmt_int32((int32)PGRAC_GRD_SHARD_COUNT));
	emit_row(rsinfo, "grd", "grd_local_master_count",
			 fmt_int32((int32)cluster_grd_local_master_count()));
	emit_row(rsinfo, "grd", "grd_remote_master_count",
			 fmt_int32((int32)cluster_grd_remote_master_count()));
	emit_row(rsinfo, "grd", "grd_shard_lookup_count",
			 fmt_int64((int64)cluster_grd_shard_lookup_count()));
	emit_row(rsinfo, "grd", "grd_local_master_lookup_count",
			 fmt_int64((int64)cluster_grd_local_master_lookup_count()));
	emit_row(rsinfo, "grd", "grd_remote_master_lookup_count",
			 fmt_int64((int64)cluster_grd_remote_master_lookup_count()));
	emit_row(rsinfo, "grd", "grd_resid_encode_count",
			 fmt_int64((int64)cluster_grd_resid_encode_count()));
	emit_row(rsinfo, "grd", "grd_master_map_refresh_count",
			 fmt_int64((int64)cluster_grd_master_map_refresh_count_get()));

	/* spec-2.15 v0.3 6 NEW emit_row (3 derived + 3 atomic;
	 * holder/waiter/convert counter 推 spec-2.16). */
	emit_row(rsinfo, "grd", "grd_max_entries", fmt_int32((int32)cluster_grd_max_entries_get()));
	emit_row(rsinfo, "grd", "grd_entry_count", fmt_int32((int32)cluster_grd_entry_count()));
	emit_row(rsinfo, "grd", "grd_allocated_bytes", fmt_int64((int64)cluster_grd_allocated_bytes()));
	emit_row(rsinfo, "grd", "grd_entry_create_count",
			 fmt_int64((int64)cluster_grd_entry_create_count()));
	emit_row(rsinfo, "grd", "grd_entry_lookup_hit_count",
			 fmt_int64((int64)cluster_grd_entry_lookup_hit_count()));
	emit_row(rsinfo, "grd", "grd_entry_full_count",
			 fmt_int64((int64)cluster_grd_entry_full_count()));

	emit_row(rsinfo, "grd", "grd_holders_full_count",
			 fmt_int64((int64)cluster_grd_holders_full_count()));
	emit_row(rsinfo, "grd", "grd_waiters_full_count",
			 fmt_int64((int64)cluster_grd_waiters_full_count()));
	emit_row(rsinfo, "grd", "grd_converts_full_count",
			 fmt_int64((int64)cluster_grd_converts_full_count()));
	emit_row(rsinfo, "grd", "grd_ngranted_promoted_count",
			 fmt_int64((int64)cluster_grd_ngranted_promoted_count()));
	emit_row(rsinfo, "grd", "grd_ges_work_queue_full_count",
			 fmt_int64((int64)cluster_grd_ges_work_queue_full_count()));
	emit_row(rsinfo, "grd", "grd_ges_cleanup_deferred_count",
			 fmt_int64((int64)cluster_grd_ges_cleanup_deferred_count()));
	emit_row(rsinfo, "grd", "grd_ges_inbound_validation_fail_count",
			 fmt_int64((int64)cluster_grd_ges_inbound_validation_fail_count()));
	emit_row(rsinfo, "grd", "grd_ges_reply_deferred_count",
			 fmt_int64((int64)cluster_grd_ges_reply_deferred_count()));
	emit_row(rsinfo, "grd", "grd_ges_reply_dropped_count",
			 fmt_int64((int64)cluster_grd_ges_reply_dropped_count()));
	emit_row(rsinfo, "grd", "grd_outbound_ring_depth",
			 fmt_int32((int32)cluster_grd_outbound_ring_depth()));
	emit_row(rsinfo, "grd", "grd_outbound_reply_dirty_depth",
			 fmt_int32((int32)cluster_grd_outbound_reply_dirty_depth()));
	emit_row(rsinfo, "grd", "grd_outbound_cleanup_dirty_depth",
			 fmt_int32((int32)cluster_grd_outbound_cleanup_dirty_depth()));
	emit_row(rsinfo, "grd", "grd_work_queue_depth",
			 fmt_int32((int32)cluster_grd_work_queue_depth()));
	emit_row(rsinfo, "grd", "grd_pending_count", fmt_int64((int64)cluster_grd_pending_count()));

	/* spec-2.17 D27 — BAST 6 counter + deadlock 3 counter(9 NEW row). */
	emit_row(rsinfo, "grd", "grd_bast_sent_count", fmt_int64((int64)cluster_grd_bast_sent_count()));
	emit_row(rsinfo, "grd", "grd_bast_received_count",
			 fmt_int64((int64)cluster_grd_bast_received_count()));
	emit_row(rsinfo, "grd", "grd_bast_ack_count", fmt_int64((int64)cluster_grd_bast_ack_count()));
	emit_row(rsinfo, "grd", "grd_bast_retry_count",
			 fmt_int64((int64)cluster_grd_bast_retry_count()));
	emit_row(rsinfo, "grd", "grd_bast_reject_count",
			 fmt_int64((int64)cluster_grd_bast_reject_count()));
	emit_row(rsinfo, "grd", "grd_bast_stale_drop_count",
			 fmt_int64((int64)cluster_grd_bast_stale_drop_count()));
	emit_row(rsinfo, "grd", "grd_deadlock_probe_drop_count",
			 fmt_int64((int64)cluster_grd_deadlock_probe_drop_count()));
	emit_row(rsinfo, "grd", "grd_deadlock_probe_collision_drop_count",
			 fmt_int64((int64)cluster_grd_deadlock_probe_collision_drop_count()));
	emit_row(rsinfo, "grd", "grd_deadlock_chunk_oo_buffer_overflow_count",
			 fmt_int64((int64)cluster_grd_deadlock_chunk_oo_buffer_overflow_count()));
}

/*
 * dump_lms -- spec-2.18 Sprint A Step 4 D10.
 *
 *	Emits 6 rows under category='lms' corresponding to the 6 atomic
 *	counters in ClusterLmsSharedState (v0.3 §1.4 F2 收紧;
 *	grant/reject/convert 分项 counter 推 spec-2.20 真激活 grant state
 *	machine 时一并 ship).
 *
 *	Plus the LMS state string for HC2 4-state semantic分流
 *	observability.
 */
static void
dump_lms(ReturnSetInfo *rsinfo)
{
	ClusterLmsState s = cluster_lms_get_state();

	emit_row(rsinfo, "lms", "lms_state", cluster_lms_state_to_string(s));
	emit_row(rsinfo, "lms", "lms_started_count", fmt_int64((int64)cluster_lms_get_started_count()));
	emit_row(rsinfo, "lms", "lms_work_drained_count",
			 fmt_int64((int64)cluster_lms_get_work_drained_count()));
	emit_row(rsinfo, "lms", "lms_decision_count",
			 fmt_int64((int64)cluster_lms_get_decision_count()));
	emit_row(rsinfo, "lms", "lms_drain_empty_count",
			 fmt_int64((int64)cluster_lms_get_drain_empty_count()));
	emit_row(rsinfo, "lms", "lms_error_count", fmt_int64((int64)cluster_lms_get_error_count()));
}


/*
 * dump_ges -- spec-2.13 D4 GES protocol skeleton observability.
 *
 *	Emits 2 rows under category='ges':
 *	  - ges_request_defer_count:  bumped on every GES_REQUEST handler
 *	    stub call (永远 DEFER per Q4.1).
 *	  - ges_reply_defer_count:    bumped on every GES_REPLY handler
 *	    stub call.
 *
 *	Skeleton phase (spec-2.13 ship):  no caller-side send (Q4.2 NONE
 *	producer_mask),  so both counters stay 0 in production.  Future
 *	spec-2.14+ caller-side bumps these on real GES traffic;  spec-2.15+
 *	splits them per state (GRANTED / WAITING / CONVERTING / DEADLOCK).
 */
static void
dump_ges(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "ges", "ges_request_defer_count",
			 fmt_int64((int64)cluster_ges_request_defer_count()));
	emit_row(rsinfo, "ges", "ges_reply_defer_count",
			 fmt_int64((int64)cluster_ges_reply_defer_count()));
}


/*
 * dump_shared_fs -- Stage 1.1 cluster_shared_fs runtime state.
 *
 *	Emits two rows: the active backend's name (or "(none)" if init has
 *	not yet run -- only happens in disable-cluster code paths or very
 *	early postmaster lifetimes that should not reach this SRF), and a
 *	CSV of every backend currently in the registry.  Runs lock-free
 *	against process-local state set up by cluster_shared_fs_init.
 */
static void
dump_shared_fs(ReturnSetInfo *rsinfo)
{
	const ClusterSharedFsOps *active = cluster_shared_fs_get_active_ops();
	StringInfoData csv;
	int i;
	int emitted = 0;

	emit_row(rsinfo, "shared_fs", "active_backend", active != NULL ? active->name : "(none)");

	initStringInfo(&csv);
	for (i = 0; i < CLUSTER_SHARED_FS_BACKEND_MAX; i++) {
		const ClusterSharedFsOps *ops = cluster_shared_fs_get_backend_at(i);

		if (ops == NULL)
			continue;
		if (emitted > 0)
			appendStringInfoChar(&csv, ',');
		appendStringInfoString(&csv, ops->name);
		emitted++;
	}
	emit_row(rsinfo, "shared_fs", "registered_backends", csv.len > 0 ? csv.data : "(empty)");
	pfree(csv.data);

	/* Stage 1.2 cluster_smgr extension: surface the routing GUC + the
	 * count of cluster_smgr SMgrRelations live in the bypass HTAB. */
	emit_row(rsinfo, "shared_fs", "smgr_user_relations", fmt_bool(cluster_smgr_user_relations));
	emit_row(rsinfo, "shared_fs", "smgr_active_relations",
			 fmt_int32(cluster_smgr_active_relation_count()));
}

/*
 * dump_block_format -- Stage 1.4 page header / SCN type metadata
 *	+ Stage 1.5 ITL slot array / tuple header invariants.
 *
 *	Emits 9 rows surfacing the spec-1.4 + spec-1.5 block format
 *	invariants so DBA can verify the binary's expectations against
 *	disk via pageinspect.  These are compile-time constants in this
 *	build; the values flag any future binary that fails to bump one
 *	when the layout actually changes (a real risk during pg_upgrade
 *	work in spec-1.25).
 */
static void
dump_block_format(ReturnSetInfo *rsinfo)
{
	/* Stage 1.4 invariants (4 keys). */
	emit_row(rsinfo, "block_format", "page_layout_version", fmt_int32(PG_PAGE_LAYOUT_VERSION));
	emit_row(rsinfo, "block_format", "page_header_size", fmt_int32((int32)SizeOfPageHeaderData));
	emit_row(rsinfo, "block_format", "scn_size_bytes", fmt_int32((int32)sizeof(SCN)));
	emit_row(rsinfo, "block_format", "invalid_scn_value", "0");

	/* Stage 1.5 ITL slot + tuple header invariants (5 keys).
	 * PIVOT A (2026-05-02): ITL is in PG special area at page tail,
	 * not after PageHeader.  itl_location key surfaces this fact for
	 * DBA diagnostic use; itl_special_size_bytes (= 384) is the
	 * special-area space carved out by PageInitHeapPage. */
	emit_row(rsinfo, "block_format", "itl_slot_size_bytes", fmt_int32(CLUSTER_ITL_SLOT_SIZE));
	emit_row(rsinfo, "block_format", "itl_initrans_default",
			 fmt_int32(CLUSTER_ITL_INITRANS_DEFAULT));
	emit_row(rsinfo, "block_format", "itl_array_bytes", fmt_int32(CLUSTER_ITL_ARRAY_SIZE));
	emit_row(rsinfo, "block_format", "tuple_header_extra_bytes", "1");
	emit_row(rsinfo, "block_format", "itl_location", "page_special_area_tail");
}

/*
 * dump_buffer_format -- Stage 1.6 buffer descriptor cluster fields layout.
 *
 *	Emits 6 rows surfacing the spec-1.6 BufferDesc layout invariants.
 *	Reports actual sizeof / offsetof values (not compile-time guesses)
 *	so DBAs can verify the binary's layout matches expectations.
 *
 *	PIVOT B (2026-05-02): on PG 16.13 sizeof(BufferTag) == 20 (not 16),
 *	pushing PG-original fields to offset 52 and leaving only 12B of
 *	cache line 1 for the cluster hot tail.  block_scn occupies cache
 *	line 1 (Stage 2-3 visibility hot path); cr_chain_head moved to
 *	cache line 2 boundary.  Spec-1.6 5 StaticAssertDecl in
 *	buf_internals.h enforce these invariants at compile time.
 */
static void
dump_buffer_format(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "buffer_format", "buffer_desc_size_bytes",
			 fmt_int32((int32)sizeof(BufferDesc)));
	emit_row(rsinfo, "buffer_format", "buffer_desc_pad_to_size", fmt_int32(BUFFERDESC_PAD_TO_SIZE));
	emit_row(rsinfo, "buffer_format", "buffer_hot_field_offset",
			 fmt_int32((int32)offsetof(BufferDesc, buffer_type)));
	emit_row(rsinfo, "buffer_format", "buffer_cold_field_offset",
			 fmt_int32((int32)offsetof(BufferDesc, cr_chain_head)));
	emit_row(rsinfo, "buffer_format", "buffer_type_count", "3");
	emit_row(rsinfo, "buffer_format", "pcm_state_count", "3");
}


/*
 * dump_pcm -- Stage 1.7 PCM lock framework scaffolding diagnostics.
 *
 *	Emits 6 rows surfacing the spec-1.7 PCM stub state for DBA
 *	visibility.  pcm_api_state="stub" makes it obvious that PCM lock
 *	manager is in scaffolding mode (not yet truth-activated).
 *
 *	Q4 user 修订 2026-05-02 added pcm_grd_allocated_bytes (actual
 *	shmem occupancy) and pcm_api_state (so DBAs aren't surprised by
 *	0A000 errors when calling PCM lock functions).
 *
 *	Spec: spec-1.7-pcm-state-placeholder.md §1.2 Deliverable 4 +
 *	      §11.4 pg_cluster_state.pcm checklist.
 */
static void
dump_pcm(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "pcm", "pcm_grd_max_entries", fmt_int32(cluster_pcm_grd_max_entries));
	/*
	 * fmt_int64 (not int32) per codex 1.7 review P3: Stage 2 default
	 * NBuffers + large shared_buffers can exceed 32-bit signed range.
	 * Spec: spec-1.X-cluster-smgr-hardening §1.3.1 finding #4.
	 */
	emit_row(rsinfo, "pcm", "pcm_grd_allocated_bytes",
			 fmt_int64((int64)cluster_pcm_grd_shmem_size()));
	emit_row(rsinfo, "pcm", "pcm_grd_active_entries", fmt_int32(cluster_pcm_grd_count()));
	emit_row(rsinfo, "pcm", "pcm_lock_mode_count", "3");
	emit_row(rsinfo, "pcm", "pcm_transition_count", fmt_int32(PCM_TRANSITION_COUNT));
	emit_row(rsinfo, "pcm", "pcm_api_state", "stub");
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
		dump_shared_fs(rsinfo);
		dump_block_format(rsinfo);
		dump_buffer_format(rsinfo);
		dump_pcm(rsinfo);
		dump_phase(rsinfo);
		dump_lmon(rsinfo);
		dump_lck(rsinfo);
		dump_diag(rsinfo);
		dump_cluster_stats(rsinfo);
		dump_cluster_cssd(rsinfo);
		dump_scn(rsinfo);
		dump_ges(rsinfo);
		dump_grd(rsinfo);
		dump_lms(rsinfo);
	}
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_state requires --enable-cluster")));
#endif

	return (Datum)0;
}
