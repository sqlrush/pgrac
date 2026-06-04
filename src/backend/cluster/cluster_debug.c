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
#include "cluster/cluster_ges_reply_wait.h" /* spec-2.23 D13 reply wait counters */
#include "cluster/cluster_grd.h"	 /* cluster_grd_* observability accessors (spec-2.14 D6) */
#include "cluster/cluster_lmd.h"	 /* cluster_lmd_* observability accessors (spec-2.19 D10) */
#include "cluster/cluster_lms.h"	 /* cluster_lms_* observability accessors (spec-2.18 D10) */
#include "cluster/cluster_tt_slot.h" /* spec-3.12 D5 retention counters */
#include "cluster/cluster_undo_record_api.h" /* cluster_undo_* counter accessors (spec-3.7 D10) */
#include "cluster/cluster_cr.h"				 /* cluster_cr_* counter accessors (spec-3.9 D8) */
#include "cluster/cluster_tt_durable.h"		 /* cluster_tt_durable_* counters (spec-3.11 D8) */
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_grd_pending.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_cssd.h"		  /* cluster_cssd_status (spec-2.5 D12) */
#include "cluster/cluster_stats.h"		  /* cluster_stats_status (spec-1.14 D12) */
#include "cluster/cluster_undo_cleaner.h" /* dump_undo_cleaner (spec-3.13 D1) */
#include "cluster/cluster_lmon.h"		  /* cluster_lmon_status (spec-1.11 Sprint B D12) */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"				/* ClusterICOps_Active, ClusterICTier */
#include "cluster/cluster_ic_tier1.h"		/* listener metadata accessors (Hardening v1.0.1 F3) */
#include "cluster/cluster_scn.h"			/* SCN typedef (stage 1.4) */
#include "cluster/cluster_itl_slot.h"		/* CLUSTER_ITL_* constants (stage 1.5) */
#include "cluster/cluster_buffer_desc.h"	/* BufferType / PcmState enums (stage 1.6) */
#include "cluster/cluster_pcm_lock.h"		/* PCM state-machine API + grd helpers */
#include "cluster/cluster_gcs.h"			/* GCS request protocol surface (spec-2.32 D8) */
#include "cluster/cluster_gcs_block.h"		/* GCS block-ship data plane (spec-2.33 D10) */
#include "cluster/cluster_sinval.h"			/* SI Broadcaster counter accessors (spec-2.38 D10) */
#include "cluster/cluster_tt_status.h"		/* TT status overlay counter accessors (spec-3.1 D9) */
#include "cluster/cluster_tt_status_hint.h" /* TT status hint counter accessors (spec-3.2 D8) */
#include "cluster/cluster_startup_phase.h"	/* phase enum + accessors (stage 1.10) */
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
 * dump_undo_cleaner -- Stage 3.13 Undo Cleaner aux process state
 * diagnostics (mirrors dump_cluster_stats F11 7-key model: 2 status +
 * 5 lifecycle).
 */
static void
dump_undo_cleaner(ReturnSetInfo *rsinfo)
{
	UndoCleanerStatus s = cluster_undo_cleaner_status();
	pid_t pid;
	TimestampTz spawned_at, ready_at, last_tick;
	int64 iters;

	emit_row(rsinfo, "undo_cleaner", "undo_cleaner_status",
			 cluster_undo_cleaner_status_to_string(s));
	emit_row(rsinfo, "undo_cleaner", "undo_cleaner_status_enum_value", fmt_int32((int32)s));

	pid = cluster_undo_cleaner_pid();
	emit_row(rsinfo, "undo_cleaner", "undo_cleaner_pid",
			 pid == 0 ? "(unset)" : fmt_int64((int64)pid));

	spawned_at = cluster_undo_cleaner_spawned_at();
	emit_row(rsinfo, "undo_cleaner", "undo_cleaner_spawned_at",
			 spawned_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(spawned_at)));

	ready_at = cluster_undo_cleaner_ready_at();
	emit_row(rsinfo, "undo_cleaner", "undo_cleaner_ready_at",
			 ready_at == 0 ? "(unset)" : pstrdup(timestamptz_to_str(ready_at)));

	last_tick = cluster_undo_cleaner_last_liveness_tick_at();
	emit_row(rsinfo, "undo_cleaner", "undo_cleaner_last_liveness_tick_at",
			 last_tick == 0 ? "(unset)" : pstrdup(timestamptz_to_str(last_tick)));

	iters = cluster_undo_cleaner_main_loop_iters();
	emit_row(rsinfo, "undo_cleaner", "undo_cleaner_main_loop_iters", fmt_int64(iters));
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
	/* spec-2.24 D13 — cleanup_skip_stale_cancel(LMD CANCEL 4-tuple mismatch). */
	emit_row(rsinfo, "grd", "grd_cleanup_skip_stale_cancel_count",
			 fmt_int64((int64)cluster_grd_cleanup_skip_stale_cancel_count()));
	/* spec-2.25 D13 — RELATION + OBJECT cluster gate hit (HC23..HC27). */
	emit_row(rsinfo, "grd", "grd_relation_object_cluster_path_count",
			 fmt_int64((int64)cluster_grd_relation_object_cluster_path_count()));
	/* spec-2.26 D5 — TRANSACTION cluster gate hit (HC39 / HC47). */
	emit_row(rsinfo, "grd", "grd_transaction_cluster_path_count",
			 fmt_int64((int64)cluster_grd_transaction_cluster_path_count()));
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
	/*
	 * spec-2.20 D10 — 3 NEW counter (grant/reject/convert) replacing
	 * single lms_decision_count.  Mutually exclusive per decision.
	 */
	emit_row(rsinfo, "lms", "lms_decision_grant_count",
			 fmt_int64((int64)cluster_lms_get_decision_grant_count()));
	emit_row(rsinfo, "lms", "lms_decision_reject_count",
			 fmt_int64((int64)cluster_lms_get_decision_reject_count()));
	emit_row(rsinfo, "lms", "lms_decision_convert_count",
			 fmt_int64((int64)cluster_lms_get_decision_convert_count()));
	emit_row(rsinfo, "lms", "lms_drain_empty_count",
			 fmt_int64((int64)cluster_lms_get_drain_empty_count()));
	emit_row(rsinfo, "lms", "lms_error_count", fmt_int64((int64)cluster_lms_get_error_count()));

	/* spec-2.25 D13 — 7 NEW native-lock probe counter rows. */
	emit_row(rsinfo, "lms", "native_probe_sent_count",
			 fmt_int64((int64)cluster_lms_get_native_probe_sent_count()));
	emit_row(rsinfo, "lms", "native_probe_reply_recv_count",
			 fmt_int64((int64)cluster_lms_get_native_probe_reply_recv_count()));
	emit_row(rsinfo, "lms", "native_probe_collector_slot_full_count",
			 fmt_int64((int64)cluster_lms_get_native_probe_collector_slot_full_count()));
	emit_row(rsinfo, "lms", "native_probe_aggregate_holder_conflict_count",
			 fmt_int64((int64)cluster_lms_get_native_probe_aggregate_holder_conflict_count()));
	emit_row(rsinfo, "lms", "native_probe_aggregate_waiter_conflict_count",
			 fmt_int64((int64)cluster_lms_get_native_probe_aggregate_waiter_conflict_count()));
	emit_row(rsinfo, "lms", "native_probe_retry_count",
			 fmt_int64((int64)cluster_lms_get_native_probe_retry_count()));
	emit_row(rsinfo, "lms", "native_probe_timeout_count",
			 fmt_int64((int64)cluster_lms_get_native_probe_timeout_count()));
	/* spec-2.27 D7 / HC54 — priority starvation observability (NOT sent
	 * on wire;  reserved opcode 11 awaits spec-2.28+ integrated receiver). */
	emit_row(rsinfo, "lms", "priority_starvation_observed_count",
			 fmt_int64((int64)cluster_lms_get_priority_starvation_observed_count()));
}

/*
 * dump_lmd -- spec-2.19 Sprint A Step 4 D10.
 *
 *	Emits 16 rows under category='lmd' (spec-2.19 daemon state/counters +
 *	spec-2.22 graph/Tarjan counters)
 *	corresponding to the LMD skeleton observability surface (HC2 4-state
 *	semantic split via state column + 6 counters per §0 Q8;
 *	add_edge / remove_edge / cycle_detected / victim_selected 分项 counter
 *	推 spec-2.20+ 真激活 Tarjan).
 *
 *	**L122 alphabetic order**:'lmd' sorts BEFORE 'lmon' in
 *	pg_cluster_state ORDER BY category(ASCII `d` 0x64 < `o` 0x6F).
 */
static void
dump_lmd(ReturnSetInfo *rsinfo)
{
	ClusterLmdState s = cluster_lmd_get_state();

	emit_row(rsinfo, "lmd", "lmd_state", cluster_lmd_state_to_string(s));
	emit_row(rsinfo, "lmd", "lmd_started_count", fmt_int64((int64)cluster_lmd_get_started_count()));
	emit_row(rsinfo, "lmd", "lmd_ready_at_us", fmt_int64((int64)cluster_lmd_get_ready_at()));
	emit_row(rsinfo, "lmd", "lmd_edge_submission_count",
			 fmt_int64((int64)cluster_lmd_get_edge_submission_count()));
	emit_row(rsinfo, "lmd", "lmd_wake_count", fmt_int64((int64)cluster_lmd_get_wake_count()));
	emit_row(rsinfo, "lmd", "lmd_idle_count", fmt_int64((int64)cluster_lmd_get_idle_count()));
	emit_row(rsinfo, "lmd", "lmd_error_count", fmt_int64((int64)cluster_lmd_get_error_count()));

	/* spec-2.22 D12 — 9 NEW counter rows (real Tarjan + graph + injection). */
	emit_row(rsinfo, "lmd", "wait_edge_count", fmt_int64((int64)cluster_lmd_wait_edge_count_get()));
	emit_row(rsinfo, "lmd", "wait_edge_full_count",
			 fmt_int64((int64)cluster_lmd_wait_edge_full_count_get()));
	emit_row(rsinfo, "lmd", "graph_generation",
			 fmt_int64((int64)cluster_lmd_graph_generation_get()));
	emit_row(rsinfo, "lmd", "tarjan_scan_count",
			 fmt_int64((int64)cluster_lmd_tarjan_scan_count_get()));
	emit_row(rsinfo, "lmd", "cycle_detected_count",
			 fmt_int64((int64)cluster_lmd_cycle_detected_count_get()));
	emit_row(rsinfo, "lmd", "victim_cancel_sent_count",
			 fmt_int64((int64)cluster_lmd_victim_cancel_sent_count_get()));
	emit_row(rsinfo, "lmd", "revalidate_fail_count",
			 fmt_int64((int64)cluster_lmd_revalidate_fail_count_get()));
	emit_row(rsinfo, "lmd", "cross_node_victim_pending_count",
			 fmt_int64((int64)cluster_lmd_cross_node_victim_pending_count_get()));
	emit_row(rsinfo, "lmd", "inject_call_count",
			 fmt_int64((int64)cluster_lmd_inject_call_count_get()));
	/* spec-2.23 D13 — 2 NEW coordinator probe counters. */
	emit_row(rsinfo, "lmd", "probe_broadcast_count",
			 fmt_int64((int64)cluster_lmd_probe_broadcast_count_get()));
	emit_row(rsinfo, "lmd", "probe_partial_count",
			 fmt_int64((int64)cluster_lmd_probe_partial_count_get()));
	/* spec-2.24 D13 — 6 NEW counters (D + cleanup axes). */
	emit_row(rsinfo, "lmd", "cleanup_lmd_sweep_count",
			 fmt_int64((int64)cluster_lmd_cleanup_lmd_sweep_count_get()));
	emit_row(rsinfo, "lmd", "cleanup_on_backend_exit_count",
			 fmt_int64((int64)cluster_lmd_cleanup_on_backend_exit_count_get()));
	emit_row(rsinfo, "lmd", "cleanup_skip_other_owner_count",
			 fmt_int64((int64)cluster_lmd_cleanup_skip_other_owner_count_get()));
	emit_row(rsinfo, "lmd", "cross_node_cancel_queue_full_count",
			 fmt_int64((int64)cluster_lmd_cross_node_cancel_queue_full_count_get()));
	emit_row(rsinfo, "lmd", "cross_node_cancel_received_count",
			 fmt_int64((int64)cluster_lmd_cross_node_cancel_received_count_get()));
	emit_row(rsinfo, "lmd", "cross_node_victim_cancel_sent_count",
			 fmt_int64((int64)cluster_lmd_cross_node_victim_cancel_sent_count_get()));
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
	/*
	 * spec-2.23 D13 — 3 NEW counters for the cross-node reply wait HTAB.
	 *
	 *	reply_wait_table_active:  live entry count (HC17 5-tuple HTAB).
	 *	reply_late_drop:          late reply observed after entry deleted.
	 *	release_ack:              successful GES_RELEASE round-trip ACKs.
	 *
	 *	BAST lifecycle counters (grd_bast_sent / grd_bast_received /
	 *	grd_bast_ack) remain the SSOT in dump_grd (spec-2.17 ship);
	 *	dump_ges 不 duplicate them per FU-3 contract.
	 */
	emit_row(rsinfo, "ges", "ges_reply_wait_table_active",
			 fmt_int64((int64)cluster_ges_reply_wait_table_active_count()));
	emit_row(rsinfo, "ges", "ges_reply_late_drop_count",
			 fmt_int64((int64)cluster_ges_reply_late_drop_count()));
	emit_row(rsinfo, "ges", "ges_release_ack_count",
			 fmt_int64((int64)cluster_ges_release_ack_count()));
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
	/* PGRAC: spec-2.31 D6 v0.5 — BufferType enum extended from 3 to 5
	 * (added SCUR + XCUR for bufmgr content-lock PCM ownership). */
	emit_row(rsinfo, "buffer_format", "buffer_type_count", "5");
	emit_row(rsinfo, "buffer_format", "pcm_state_count", "3");
}


/*
 * dump_pcm -- PCM lock state-machine diagnostics.
 *
 *	spec-1.7 introduced the initial diagnostic surface.  spec-2.30 expands
 *	it with live PCM state summaries and transition counters.
 */
static void
dump_pcm(ReturnSetInfo *rsinfo)
{
	/*
	 * PGRAC: spec-2.30 D9 — dump_pcm activation surface.
	 *
	 *	Existing 6 row preserved (api_state 字符串值 "stub" → "active" 当
	 *	cluster_pcm_grd_count() > 0 或 GUC=-1 default-on;disabled path 保留
	 *	"stub").  NEW 5 state summary row + 9 transition counter row =
	 *	14 NEW (total 20).
	 */
	emit_row(rsinfo, "pcm", "pcm_grd_max_entries", fmt_int32(cluster_pcm_grd_max_entries));
	emit_row(rsinfo, "pcm", "pcm_grd_allocated_bytes",
			 fmt_int64((int64)cluster_pcm_grd_shmem_size()));
	emit_row(rsinfo, "pcm", "pcm_grd_active_entries", fmt_int32(cluster_pcm_grd_count()));
	emit_row(rsinfo, "pcm", "pcm_lock_mode_count", "3");
	emit_row(rsinfo, "pcm", "pcm_transition_count", fmt_int32(PCM_TRANSITION_COUNT));
	/*
	 * api_state: "active" if PCM 状态机已激活 (cluster.pcm_grd_max_entries
	 * non-zero, either default -1 or explicit positive);  "stub" if explicit
	 * disable (cluster.pcm_grd_max_entries=0 spec-2.30 disable path).
	 */
	emit_row(rsinfo, "pcm", "pcm_api_state",
			 (cluster_pcm_grd_max_entries == 0) ? "stub" : "active");

	/*
	 * PGRAC: spec-2.30 D9 — 5 NEW state summary row.
	 *
	 *	master_state_n_count / s_count / x_count:  iterate live HTAB,
	 *	count entries by master_state.  Could be expensive on large HTAB,
	 *	but dump_pcm is admin-on-demand surface (not hot path);  acceptable.
	 *	disable-path (htab NULL):  all 0.
	 *
	 *	pi_holders_total_count:  popcount(pi_holders_bitmap) summed across
	 *	all entries.
	 *
	 *	convert_queue_active:  count of entries with convert_queue != NULL
	 *	(spec-2.30 always 0 until spec-2.32 GCS req wires convert queue).
	 */
	{
		int n_count = 0, s_count = 0, x_count = 0;
		int pi_total = 0, convert_q_active = 0;

		cluster_pcm_grd_get_summary(&n_count, &s_count, &x_count, &pi_total, &convert_q_active);
		emit_row(rsinfo, "pcm", "master_state_n_count", fmt_int32(n_count));
		emit_row(rsinfo, "pcm", "master_state_s_count", fmt_int32(s_count));
		emit_row(rsinfo, "pcm", "master_state_x_count", fmt_int32(x_count));
		emit_row(rsinfo, "pcm", "pi_holders_total_count", fmt_int32(pi_total));
		emit_row(rsinfo, "pcm", "convert_queue_active", fmt_int32(convert_q_active));
	}

	/*
	 * PGRAC: spec-2.30 D9 — 9 NEW transition counter row.
	 *
	 *	Trans-9 (s_to_x_cleanout) accessor exists but counter永 0 in
	 *	spec-2.30 (HC60 apply-fail-closed until Stage 3 AD-006 第五轮
	 *	wires ITL cleanout).
	 */
	emit_row(rsinfo, "pcm", "trans_n_to_s_count",
			 fmt_int64((int64)cluster_pcm_get_trans_n_to_s_count()));
	emit_row(rsinfo, "pcm", "trans_n_to_x_count",
			 fmt_int64((int64)cluster_pcm_get_trans_n_to_x_count()));
	emit_row(rsinfo, "pcm", "trans_s_to_x_upgrade_count",
			 fmt_int64((int64)cluster_pcm_get_trans_s_to_x_upgrade_count()));
	emit_row(rsinfo, "pcm", "trans_x_to_s_downgrade_count",
			 fmt_int64((int64)cluster_pcm_get_trans_x_to_s_downgrade_count()));
	emit_row(rsinfo, "pcm", "trans_x_to_n_downgrade_count",
			 fmt_int64((int64)cluster_pcm_get_trans_x_to_n_downgrade_count()));
	emit_row(rsinfo, "pcm", "trans_x_to_n_release_count",
			 fmt_int64((int64)cluster_pcm_get_trans_x_to_n_release_count()));
	emit_row(rsinfo, "pcm", "trans_s_to_n_invalidate_count",
			 fmt_int64((int64)cluster_pcm_get_trans_s_to_n_invalidate_count()));
	emit_row(rsinfo, "pcm", "trans_s_to_n_release_count",
			 fmt_int64((int64)cluster_pcm_get_trans_s_to_n_release_count()));
	emit_row(rsinfo, "pcm", "trans_s_to_x_cleanout_count",
			 fmt_int64((int64)cluster_pcm_get_trans_s_to_x_cleanout_count()));
}


/* ============================================================
 * dump_gcs -- GCS request protocol observability (spec-2.32 D8 + spec-2.33 D10).
 *
 *	22 rows total = 14 control-plane rows (spec-2.32) + 8 data-plane rows
 *	(spec-2.33 D10):  block_request_count + block_reply_count +
 *	block_timeout_count + block_checksum_fail_count +
 *	block_storage_fallback_count + block_master_not_holder_count +
 *	block_wal_flush_before_ship_count + block_ship_bytes_total.
 * ============================================================ */
static void
dump_gcs(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "gcs", "api_state", cluster_gcs_get_api_state());
	emit_row(rsinfo, "gcs", "lookup_master_self_count",
			 fmt_int64((int64)cluster_gcs_get_lookup_master_self_count()));
	emit_row(rsinfo, "gcs", "lookup_master_remote_count",
			 fmt_int64((int64)cluster_gcs_get_lookup_master_remote_count()));
	emit_row(rsinfo, "gcs", "send_request_count",
			 fmt_int64((int64)cluster_gcs_get_send_request_count()));
	emit_row(rsinfo, "gcs", "handle_request_count",
			 fmt_int64((int64)cluster_gcs_get_handle_request_count()));
	emit_row(rsinfo, "gcs", "handle_reply_count",
			 fmt_int64((int64)cluster_gcs_get_handle_reply_count()));
	emit_row(rsinfo, "gcs", "reply_late_drop_count",
			 fmt_int64((int64)cluster_gcs_get_reply_late_drop_count()));
	emit_row(rsinfo, "gcs", "reply_timeout_count",
			 fmt_int64((int64)cluster_gcs_get_reply_timeout_count()));
	emit_row(rsinfo, "gcs", "encode_payload_bytes",
			 fmt_int64((int64)cluster_gcs_get_encode_payload_bytes()));
	emit_row(rsinfo, "gcs", "decode_payload_bytes",
			 fmt_int64((int64)cluster_gcs_get_decode_payload_bytes()));
	emit_row(rsinfo, "gcs", "dispatch_loop_iterations",
			 fmt_int64((int64)cluster_gcs_get_dispatch_loop_iterations()));
	emit_row(rsinfo, "gcs", "outstanding_count",
			 fmt_int64((int64)cluster_gcs_get_outstanding_count()));
	emit_row(rsinfo, "gcs", "max_outstanding", fmt_int64((int64)cluster_gcs_get_max_outstanding()));
	emit_row(rsinfo, "gcs", "max_outstanding_per_backend",
			 fmt_int32(MAX_OUTSTANDING_REQUESTS_PER_BACKEND));

	/* spec-2.33 D10:  8 NEW data-plane counter rows (block ship substrate). */
	emit_row(rsinfo, "gcs", "block_request_count",
			 fmt_int64((int64)cluster_gcs_get_block_request_count()));
	emit_row(rsinfo, "gcs", "block_reply_count",
			 fmt_int64((int64)cluster_gcs_get_block_reply_count()));
	emit_row(rsinfo, "gcs", "block_timeout_count",
			 fmt_int64((int64)cluster_gcs_get_block_timeout_count()));
	emit_row(rsinfo, "gcs", "block_checksum_fail_count",
			 fmt_int64((int64)cluster_gcs_get_block_checksum_fail_count()));
	emit_row(rsinfo, "gcs", "block_storage_fallback_count",
			 fmt_int64((int64)cluster_gcs_get_block_storage_fallback_count()));
	emit_row(rsinfo, "gcs", "block_master_not_holder_count",
			 fmt_int64((int64)cluster_gcs_get_block_master_not_holder_count()));
	emit_row(rsinfo, "gcs", "block_wal_flush_before_ship_count",
			 fmt_int64((int64)cluster_gcs_get_block_wal_flush_before_ship_count()));
	emit_row(rsinfo, "gcs", "block_ship_bytes_total",
			 fmt_int64((int64)cluster_gcs_get_block_ship_bytes_total()));

	/* spec-2.34 D10:  9 NEW reliability hardening counter rows
	 * (dump_gcs 22 → 31 row).  Mirrors counters in
	 * ClusterGcsBlockShared (5 sender/wake) + cluster_gcs_block_dedup
	 * (4 dedup HTAB). */
	emit_row(rsinfo, "gcs", "retransmit_attempt_count",
			 fmt_int64((int64)cluster_gcs_get_block_retransmit_attempt_count()));
	emit_row(rsinfo, "gcs", "retransmit_send_count",
			 fmt_int64((int64)cluster_gcs_get_block_retransmit_send_count()));
	emit_row(rsinfo, "gcs", "retransmit_exhausted_count",
			 fmt_int64((int64)cluster_gcs_get_block_retransmit_exhausted_count()));
	emit_row(rsinfo, "gcs", "dedup_hit_count",
			 fmt_int64((int64)cluster_gcs_get_block_dedup_hit_count()));
	emit_row(rsinfo, "gcs", "dedup_miss_count",
			 fmt_int64((int64)cluster_gcs_get_block_dedup_miss_count()));
	emit_row(rsinfo, "gcs", "dedup_collision_count",
			 fmt_int64((int64)cluster_gcs_get_block_dedup_collision_count()));
	emit_row(rsinfo, "gcs", "dedup_full_count",
			 fmt_int64((int64)cluster_gcs_get_block_dedup_full_count()));
	emit_row(rsinfo, "gcs", "epoch_invalidate_wake_count",
			 fmt_int64((int64)cluster_gcs_get_block_epoch_invalidate_wake_count()));
	emit_row(rsinfo, "gcs", "stale_reply_drop_count",
			 fmt_int64((int64)cluster_gcs_get_block_stale_reply_drop_count()));

	/* PGRAC: spec-2.35 D13 — 7 NEW counter rows for CF 2-way protocol. */
	emit_row(rsinfo, "gcs", "block_forward_sent_count",
			 fmt_int64((int64)cluster_gcs_get_block_forward_sent_count()));
	emit_row(rsinfo, "gcs", "block_forward_received_count",
			 fmt_int64((int64)cluster_gcs_get_block_forward_received_count()));
	emit_row(rsinfo, "gcs", "block_from_holder_ship_count",
			 fmt_int64((int64)cluster_gcs_get_block_from_holder_ship_count()));
	emit_row(rsinfo, "gcs", "block_forward_holder_evicted_count",
			 fmt_int64((int64)cluster_gcs_get_block_forward_holder_evicted_count()));
	emit_row(rsinfo, "gcs", "s_holders_bitmap_redirect_count",
			 fmt_int64((int64)cluster_gcs_get_block_s_holders_bitmap_redirect_count()));
	emit_row(rsinfo, "gcs", "master_holder_lifecycle_count",
			 fmt_int64((int64)cluster_gcs_get_block_master_holder_lifecycle_count()));
	emit_row(rsinfo, "gcs", "forward_replay_count",
			 fmt_int64((int64)cluster_gcs_get_block_forward_replay_count()));

	/* PGRAC: spec-2.36 D10 — 6 NEW counter rows for CF 3-way protocol. */
	emit_row(rsinfo, "gcs", "block_invalidate_broadcast_count",
			 fmt_int64((int64)cluster_gcs_get_block_invalidate_broadcast_count()));
	emit_row(rsinfo, "gcs", "block_invalidate_ack_received_count",
			 fmt_int64((int64)cluster_gcs_get_block_invalidate_ack_received_count()));
	emit_row(rsinfo, "gcs", "block_invalidate_timeout_count",
			 fmt_int64((int64)cluster_gcs_get_block_invalidate_timeout_count()));
	emit_row(rsinfo, "gcs", "block_x_forward_sent_count",
			 fmt_int64((int64)cluster_gcs_get_block_x_forward_sent_count()));
	emit_row(rsinfo, "gcs", "block_x_granted_from_holder_count",
			 fmt_int64((int64)cluster_gcs_get_block_x_granted_from_holder_count()));
	emit_row(rsinfo, "gcs", "starvation_denied_pending_x_count",
			 fmt_int64((int64)cluster_gcs_get_starvation_denied_pending_x_count()));

	/* PGRAC: spec-2.37 D12 — 4 NEW counter rows for PI watermark + lost-write. */
	emit_row(rsinfo, "gcs", "pi_watermark_advance_count",
			 fmt_int64((int64)cluster_gcs_get_pi_watermark_advance_count()));
	emit_row(rsinfo, "gcs", "pi_watermark_retire_count",
			 fmt_int64((int64)cluster_gcs_get_pi_watermark_retire_count()));
	emit_row(rsinfo, "gcs", "lost_write_detected_count",
			 fmt_int64((int64)cluster_gcs_get_lost_write_detected_count()));
	emit_row(rsinfo, "gcs", "lost_write_avoid_count",
			 fmt_int64((int64)cluster_gcs_get_lost_write_avoid_count()));

	/* PGRAC: spec-2.38 D10 — 9 NEW counter rows for SI Broadcaster. */
	emit_row(rsinfo, "sinval", "broadcast_send_count",
			 fmt_int64((int64)cluster_sinval_get_broadcast_send_count()));
	emit_row(rsinfo, "sinval", "broadcast_receive_count",
			 fmt_int64((int64)cluster_sinval_get_broadcast_receive_count()));
	emit_row(rsinfo, "sinval", "inject_local_queue_count",
			 fmt_int64((int64)cluster_sinval_get_inject_local_queue_count()));
	emit_row(rsinfo, "sinval", "outbound_queue_full_count",
			 fmt_int64((int64)cluster_sinval_get_outbound_queue_full_count()));
	emit_row(rsinfo, "sinval", "inbound_queue_full_count",
			 fmt_int64((int64)cluster_sinval_get_inbound_queue_full_count()));
	emit_row(rsinfo, "sinval", "inbound_overflow_reset_count",
			 fmt_int64((int64)cluster_sinval_get_inbound_overflow_reset_count()));
	emit_row(rsinfo, "sinval", "validation_drop_count",
			 fmt_int64((int64)cluster_sinval_get_validation_drop_count()));
	emit_row(rsinfo, "sinval", "stale_epoch_drop_count",
			 fmt_int64((int64)cluster_sinval_get_stale_epoch_drop_count()));
	emit_row(rsinfo, "sinval", "echo_dropped_count",
			 fmt_int64((int64)cluster_sinval_get_echo_dropped_count()));
	/* spec-2.39 D8/D9:  6 NEW counter rows — 3 fanout partial-fail + 3 ack. */
	emit_row(rsinfo, "sinval", "fanout_would_block_count",
			 fmt_int64((int64)cluster_sinval_get_fanout_would_block_count()));
	emit_row(rsinfo, "sinval", "fanout_hard_error_count",
			 fmt_int64((int64)cluster_sinval_get_fanout_hard_error_count()));
	emit_row(rsinfo, "sinval", "fanout_peer_down_count",
			 fmt_int64((int64)cluster_sinval_get_fanout_peer_down_count()));
	emit_row(rsinfo, "sinval", "ack_received_count",
			 fmt_int64((int64)cluster_sinval_get_ack_received_count()));
	emit_row(rsinfo, "sinval", "ack_timeout_count",
			 fmt_int64((int64)cluster_sinval_get_ack_timeout_count()));
	emit_row(rsinfo, "sinval", "ack_orphan_count",
			 fmt_int64((int64)cluster_sinval_get_ack_orphan_count()));

	/* spec-3.1 D9:  7 NEW counter rows for Undo TT status overlay. */
	emit_row(rsinfo, "tt_status", "install_count",
			 fmt_int64((int64)cluster_tt_status_get_install_count()));
	emit_row(rsinfo, "tt_status", "lookup_hit_count",
			 fmt_int64((int64)cluster_tt_status_get_lookup_hit_count()));
	emit_row(rsinfo, "tt_status", "lookup_miss_count",
			 fmt_int64((int64)cluster_tt_status_get_lookup_miss_count()));
	emit_row(rsinfo, "tt_status", "evict_count",
			 fmt_int64((int64)cluster_tt_status_get_evict_count()));
	emit_row(rsinfo, "tt_status", "flush_count",
			 fmt_int64((int64)cluster_tt_status_get_flush_count()));
	emit_row(rsinfo, "tt_status", "self_consumer_hit_count",
			 fmt_int64((int64)cluster_tt_status_get_self_consumer_hit_count()));
	emit_row(rsinfo, "tt_status", "evict_fail_count",
			 fmt_int64((int64)cluster_tt_status_get_evict_fail_count()));

	/* spec-3.2 D8:  6 NEW counter rows for cross-node TT status hint wire. */
	emit_row(rsinfo, "tt_status_hint", "emit_count",
			 fmt_int64((int64)cluster_tt_status_hint_get_emit_count()));
	emit_row(rsinfo, "tt_status_hint", "receive_count",
			 fmt_int64((int64)cluster_tt_status_hint_get_receive_count()));
	emit_row(rsinfo, "tt_status_hint", "drop_invalid_count",
			 fmt_int64((int64)cluster_tt_status_hint_get_drop_invalid_count()));
	emit_row(rsinfo, "tt_status_hint", "drop_stale_epoch_count",
			 fmt_int64((int64)cluster_tt_status_hint_get_drop_stale_epoch_count()));
	emit_row(rsinfo, "tt_status_hint", "drop_unknown_version_count",
			 fmt_int64((int64)cluster_tt_status_hint_get_drop_unknown_version_count()));
	emit_row(rsinfo, "tt_status_hint", "install_count",
			 fmt_int64((int64)cluster_tt_status_hint_get_install_count()));
	emit_row(rsinfo, "tt_status_hint", "drop_v1_compat_count",
			 fmt_int64((int64)cluster_tt_status_hint_get_drop_v1_compat_count()));
}

/*
 * dump_undo -- spec-3.7 D10 + D6 真激活 counter observability.
 *
 *	Emits 26 rows under category='undo': 5 record-level allocator counters
 *	(spec-3.7) + 4 segment-lifecycle counters (spec-3.8) + 3 commit-fsync +
 *	4 smgr counters (the latter 7 added by the perf-merge undo
 *	instrumentation) + 5 durable TT slot counters (spec-3.11 D8) + 5 retention
 *	counters (spec-3.12 D5: horizon gauge / tt_slot_retain_skip /
 *	segment_retain_skip / retention_recycle / tt_retention_rollover).  Backs
 *	cluster_tap t/213 + t/214 + t/219 L2 + t/220 verification + perf class 7
 *	baseline tracking.
 */
static void
dump_undo(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "undo", "record_alloc_count",
			 fmt_int64((int64)cluster_undo_record_alloc_count()));
	emit_row(rsinfo, "undo", "segment_claim_count",
			 fmt_int64((int64)cluster_undo_segment_claim_count()));
	emit_row(rsinfo, "undo", "block_write_count",
			 fmt_int64((int64)cluster_undo_block_write_count()));
	emit_row(rsinfo, "undo", "block_flush_count",
			 fmt_int64((int64)cluster_undo_block_flush_count()));
	emit_row(rsinfo, "undo", "reader_lookup_count",
			 fmt_int64((int64)cluster_undo_reader_lookup_count()));

	/* spec-3.8 D11: 4 NEW lifecycle counters. */
	emit_row(rsinfo, "undo", "autoextend_count", fmt_int64((int64)cluster_undo_autoextend_count()));
	emit_row(rsinfo, "undo", "segment_switch_count",
			 fmt_int64((int64)cluster_undo_segment_switch_count()));
	emit_row(rsinfo, "undo", "segment_create_fail_count",
			 fmt_int64((int64)cluster_undo_segment_create_fail_count()));
	emit_row(rsinfo, "undo", "segment_hard_cap_fail_count",
			 fmt_int64((int64)cluster_undo_segment_hard_cap_fail_count()));
	/* P0 perf hardening: per-commit (group) undo fsync observability. */
	emit_row(rsinfo, "undo", "commit_fsync_count",
			 fmt_int64((int64)cluster_undo_commit_fsync_count()));
	emit_row(rsinfo, "undo", "commit_fsync_segment_count",
			 fmt_int64((int64)cluster_undo_commit_fsync_segment_count()));
	emit_row(rsinfo, "undo", "commit_fsync_failure_count",
			 fmt_int64((int64)cluster_undo_commit_fsync_failure_count()));
	/* P0 perf hardening: undo segment-file syscall observability. */
	emit_row(rsinfo, "undo", "smgr_open_count", fmt_int64((int64)cluster_undo_smgr_open_count()));
	emit_row(rsinfo, "undo", "smgr_close_count", fmt_int64((int64)cluster_undo_smgr_close_count()));
	emit_row(rsinfo, "undo", "smgr_pread_count", fmt_int64((int64)cluster_undo_smgr_pread_count()));
	emit_row(rsinfo, "undo", "smgr_pwrite_count",
			 fmt_int64((int64)cluster_undo_smgr_pwrite_count()));

	/* spec-3.11 D8: 5 NEW durable TT slot counters. */
	emit_row(rsinfo, "undo", "tt_durable_commit_count",
			 fmt_int64((int64)cluster_tt_durable_commit_count()));
	emit_row(rsinfo, "undo", "tt_durable_lookup_hit_count",
			 fmt_int64((int64)cluster_tt_durable_lookup_hit_count()));
	emit_row(rsinfo, "undo", "tt_durable_lookup_miss_count",
			 fmt_int64((int64)cluster_tt_durable_lookup_miss_count()));
	emit_row(rsinfo, "undo", "tt_durable_by_xid_scan_count",
			 fmt_int64((int64)cluster_tt_durable_by_xid_scan_count()));
	emit_row(rsinfo, "undo", "tt_durable_redo_apply_count",
			 fmt_int64((int64)cluster_tt_durable_redo_apply_count()));

	/* spec-3.12 D5: own-instance retention horizon observability. */
	emit_row(rsinfo, "undo", "retention_horizon_scn",
			 fmt_int64((int64)cluster_tt_slot_retention_horizon_scn()));
	emit_row(rsinfo, "undo", "tt_slot_retain_skip_count",
			 fmt_int64((int64)cluster_tt_slot_retain_skip_count()));
	emit_row(rsinfo, "undo", "segment_retain_skip_count",
			 fmt_int64((int64)cluster_undo_segment_retain_skip_count()));
	emit_row(rsinfo, "undo", "retention_recycle_count",
			 fmt_int64((int64)cluster_tt_slot_retention_recycle_count()));
	emit_row(rsinfo, "undo", "tt_retention_rollover_count",
			 fmt_int64((int64)cluster_undo_tt_retention_rollover_count()));

	/* spec-3.13 D6: 6 cleaner/reuse counters (26 -> 32 rows). */
	emit_row(rsinfo, "undo", "cleaner_pass_count",
			 fmt_int64((int64)cluster_undo_cleaner_pass_count()));
	emit_row(rsinfo, "undo", "cleaner_shmem_tt_slots_gcd",
			 fmt_int64((int64)cluster_undo_cleaner_shmem_tt_slots_gcd()));
	emit_row(rsinfo, "undo", "cleaner_segments_marked_recyclable",
			 fmt_int64((int64)cluster_undo_cleaner_segments_marked_recyclable()));
	emit_row(rsinfo, "undo", "cleaner_stale_active_skipped",
			 fmt_int64((int64)cluster_undo_cleaner_stale_active_skipped()));
	emit_row(rsinfo, "undo", "segment_reuse_count",
			 fmt_int64((int64)cluster_undo_segment_reuse_count()));
	emit_row(rsinfo, "undo", "tt_slot_wrap_retired_count",
			 fmt_int64((int64)cluster_tt_slot_wrap_retired_count()));
}

/*
 * dump_cr -- spec-3.9 D8 own-instance CR counter observability.
 *
 *	Emits 9 rows under category='cr' for the cluster_cr counters.  Backs
 *	cluster_tap t/215 L2/L3/L7 verification + perf class 9 baseline.
 */
static void
dump_cr(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "cr", "cr_construct_count", fmt_int64((int64)cluster_cr_construct_count()));
	emit_row(rsinfo, "cr", "cr_snapshot_too_old_count",
			 fmt_int64((int64)cluster_cr_snapshot_too_old_count()));
	emit_row(rsinfo, "cr", "cr_cross_instance_unsupported_count",
			 fmt_int64((int64)cluster_cr_cross_instance_unsupported_count()));
	emit_row(rsinfo, "cr", "cr_corruption_count", fmt_int64((int64)cluster_cr_corruption_count()));
	emit_row(rsinfo, "cr", "cr_chain_walk_steps_sum",
			 fmt_int64((int64)cluster_cr_chain_walk_steps_sum()));
	emit_row(rsinfo, "cr", "cr_inverse_insert_count",
			 fmt_int64((int64)cluster_cr_inverse_insert_count()));
	emit_row(rsinfo, "cr", "cr_inverse_update_count",
			 fmt_int64((int64)cluster_cr_inverse_update_count()));
	emit_row(rsinfo, "cr", "cr_inverse_delete_count",
			 fmt_int64((int64)cluster_cr_inverse_delete_count()));
	emit_row(rsinfo, "cr", "cr_inverse_itl_count",
			 fmt_int64((int64)cluster_cr_inverse_itl_count()));
	/* spec-3.10 D6: CR block cache counters (cr category 9 -> 13 rows). */
	emit_row(rsinfo, "cr", "cr_cache_hit_count", fmt_int64((int64)cluster_cr_cache_hit_count()));
	emit_row(rsinfo, "cr", "cr_cache_miss_count", fmt_int64((int64)cluster_cr_cache_miss_count()));
	emit_row(rsinfo, "cr", "cr_cache_evict_count",
			 fmt_int64((int64)cluster_cr_cache_evict_count()));
	emit_row(rsinfo, "cr", "cr_cache_install_count",
			 fmt_int64((int64)cluster_cr_cache_install_count()));
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
		dump_gcs(rsinfo);
		dump_phase(rsinfo);
		dump_lmon(rsinfo);
		dump_lck(rsinfo);
		dump_diag(rsinfo);
		dump_cluster_stats(rsinfo);
		dump_cluster_cssd(rsinfo);
		dump_undo_cleaner(rsinfo);
		dump_scn(rsinfo);
		dump_ges(rsinfo);
		dump_grd(rsinfo);
		dump_lmd(rsinfo);
		dump_lms(rsinfo);
		dump_undo(rsinfo);
		dump_cr(rsinfo);
	}
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_state requires --enable-cluster")));
#endif

	return (Datum)0;
}
