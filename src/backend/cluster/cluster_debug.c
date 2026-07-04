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

#include "cluster/cluster_cf_stats.h" /* CF counters (spec-5.6 Dc4) */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_elog.h"		  /* cluster_phase */
#include "cluster/cluster_diag.h"		  /* cluster_diag_status (spec-1.13 D12) */
#include "cluster/cluster_hang.h"		  /* Hang Manager dump (spec-5.11 D4) */
#include "cluster/cluster_hang_resolve.h" /* Hang Manager disposition dump (spec-5.12 D8) */
#include "cluster/cluster_lck.h"		  /* cluster_lck_status (spec-1.12 D12) */
#include "cluster/cluster_scn.h"		  /* cluster_scn_current (spec-1.15 D6) */
#include "cluster/cluster_ges.h" /* cluster_ges_{request,reply}_defer_count (spec-2.13 D4) */
#include "cluster/cluster_ges_reply_wait.h" /* spec-2.23 D13 reply wait counters */
#include "cluster/cluster_grd.h"	  /* cluster_grd_* observability accessors (spec-2.14 D6) */
#include "cluster/cluster_hw.h"		  /* HW relation-extend authority counters (spec-5.7 §3.1c) */
#include "cluster/cluster_dl.h"		  /* DL bulk-load lease counters (spec-5.7 D4) */
#include "cluster/cluster_ir.h"		  /* IR instance-recovery owner counters (spec-5.7 D8) */
#include "cluster/cluster_ts.h"		  /* TT tablespace-DDL lock counters (spec-5.7 D5) */
#include "cluster/cluster_ko.h"		  /* KO object-reuse flush counters (spec-5.7 D6) */
#include "cluster/cluster_sequence.h" /* cluster_sq_* counters (spec-5.4 D9) */
#include "cluster/cluster_advisory.h" /* cluster_advisory_* counters (spec-5.5 D8) */
#include "cluster/cluster_lmd.h"	  /* cluster_lmd_* observability accessors (spec-2.19 D10) */
#include "cluster/cluster_lmd_probe_collector.h" /* spec-5.8 D8 — probe collector counters */
#include "cluster/cluster_lms.h"	 /* cluster_lms_* observability accessors (spec-2.18 D10) */
#include "cluster/cluster_tt_slot.h" /* spec-3.12 D5 retention counters */
#include "cluster/cluster_terminal_authority.h" /* spec-6.2 authority counters */
#include "cluster/cluster_sf_dep.h"				/* spec-6.2 Smart Fusion dep counters */
#include "cluster/cluster_undo_record_api.h"  /* cluster_undo_* counter accessors (spec-3.7 D10) */
#include "cluster/storage/cluster_undo_buf.h" /* spec-3.18 D7: undo buffer counters */
#include "cluster/cluster_cr.h"				  /* cluster_cr_* counter accessors (spec-3.9 D8) */
#include "cluster/cluster_cr_pool.h"		  /* cluster_cr_pool_* counters (spec-5.51 D9) */
#include "cluster/cluster_cr_admit.h"		  /* cluster_cr_admit_stat_* counters (spec-5.52 D9) */
#include "cluster/cluster_cr_tuple.h"		  /* cluster_cr_tuple_stat_* counters (spec-5.54 D5) */
#include "cluster/cluster_xnode_profile.h"	  /* xnode profiling buckets (spec-5.59 D1) */
#include "cluster/cluster_xnode_lever.h"	  /* xnode lever counters (spec-6.12) */
#include "cluster/cluster_hw_lease.h"		  /* space-lease counters (spec-6.12d) */
#include "cluster/cluster_resolver_cache.h"	  /* cluster_resolver_cache_* counters (spec-5.55 D8) */
#include "cluster/cluster_cr_coordinator_stat.h" /* cluster_cr_coordinator_* counters (spec-5.57 D3) */
#include "cluster/cluster_wal_state.h"			 /* wal_state registry dump (spec-4.2 D5) */
#include "cluster/cluster_wal_thread.h"			 /* wal_thread dump accessors (spec-4.1 D7) */
#include "cluster/cluster_tt_durable.h"			 /* cluster_tt_durable_* counters (spec-3.11 D8) */
#include "cluster/cluster_grd_outbound.h"
#include "cluster/cluster_grd_pending.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_cssd.h"		  /* cluster_cssd_status (spec-2.5 D12) */
#include "cluster/cluster_stats.h"		  /* cluster_stats_status (spec-1.14 D12) */
#include "cluster/cluster_undo_cleaner.h" /* dump_undo_cleaner (spec-3.13 D1) */
#include "cluster/cluster_lmon.h"		  /* cluster_lmon_status (spec-1.11 Sprint B D12) */
#include "cluster/cluster_guc.h"
#include "catalog/pg_control.h" /* DBState (spec-4.3 plan dump) */
#include "cluster/cluster_recovery_plan.h"
#include "cluster/cluster_recovery_worker.h"
#include "cluster/cluster_recovery_merge.h"	 /* is_materialized (spec-4.5a D11) */
#include "cluster/cluster_reconfig.h"		 /* spec-5.14 D6 touched counters */
#include "cluster/cluster_touched_peers.h"	 /* spec-5.14 D6 self_touched_hex */
#include "cluster/cluster_block_recovery.h"	 /* block-recovery counters (spec-4.10 D6) */
#include "cluster/cluster_thread_recovery.h" /* online thread-recovery counters (spec-4.11 D5) */
#include "cluster/cluster_write_fence.h"	 /* write-fence counters (spec-4.12 D7) */
#include "cluster/cluster_remote_xact.h"	 /* remote outcome counters (spec-4.5a D11) */
#include "cluster/cluster_ic.h"				 /* ClusterICOps_Active, ClusterICTier */
#include "cluster/cluster_ic_tier1.h"		 /* listener metadata accessors (Hardening v1.0.1 F3) */
#include "cluster/cluster_scn.h"			 /* SCN typedef (stage 1.4) */
#include "cluster/cluster_itl_slot.h"		 /* CLUSTER_ITL_* constants (stage 1.5) */
#include "cluster/cluster_buffer_desc.h"	 /* BufferType / PcmState enums (stage 1.6) */
#include "cluster/cluster_pcm_lock.h"		 /* PCM state-machine API + grd helpers */
#include "cluster/cluster_gcs.h"			 /* GCS request protocol surface (spec-2.32 D8) */
#include "cluster/cluster_gcs_block.h"		 /* GCS block-ship data plane (spec-2.33 D10) */
#include "cluster/cluster_sinval.h"			 /* SI Broadcaster counter accessors (spec-2.38 D10) */
#include "cluster/cluster_tt_status.h"		 /* TT status overlay counter accessors (spec-3.1 D9) */
#include "cluster/cluster_tt_status_hint.h"	 /* TT status hint counter accessors (spec-3.2 D8) */
#include "cluster/cluster_tx_enqueue.h"		 /* TX enqueue wait counters (spec-5.2 D4/D6) */
#include "cluster/cluster_startup_phase.h"	 /* phase enum + accessors (stage 1.10) */
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

static const char *
cf_delayed_cleanout_to_text(int mode)
{
	switch ((ClusterCfDelayedCleanoutMode)mode) {
	case CLUSTER_CF_DELAYED_CLEANOUT_OFF:
		return "off";
	case CLUSTER_CF_DELAYED_CLEANOUT_READER:
		return "reader";
	case CLUSTER_CF_DELAYED_CLEANOUT_EAGER:
		return "eager";
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
	emit_row(rsinfo, "guc", "cluster.cf_terminal_authority",
			 fmt_bool(cluster_cf_terminal_authority));
	emit_row(rsinfo, "guc", "cluster.cf_delayed_cleanout",
			 cf_delayed_cleanout_to_text(cluster_cf_delayed_cleanout));
	emit_row(rsinfo, "guc", "cluster.smart_fusion", fmt_bool(cluster_smart_fusion));
	emit_row(rsinfo, "guc", "cluster.smart_fusion_tier_min",
			 ic_tier_to_text(cluster_smart_fusion_tier_min));
	emit_row(rsinfo, "guc", "cluster.smart_fusion_commit_brake_timeout_ms",
			 fmt_int32(cluster_smart_fusion_commit_brake_timeout_ms));
	emit_row(rsinfo, "guc", "cluster.smart_fusion_origin_durable_gossip_ms",
			 fmt_int32(cluster_smart_fusion_origin_durable_gossip_ms));
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
 * dump_hang -- spec-5.11 Hang Manager diagnostics.
 *
 *	Emits the aggregate sampling state + cumulative counters, the spec-5.8
 *	aggregate deadlock context (D6 reader: cluster-wide, the only per-proc
 *	confirmed-deadlock signal shipped 5.8 exposes), and one group of per-row
 *	keys per long-wait sample.  All of it comes from a single consistent
 *	snapshot copied under the DIAG LWLock (cluster_hang_get_dump_data), so
 *	the rows have real shmem backing — never a hollow dump (spec §2.1b / R4).
 */
static void
dump_hang(ReturnSetInfo *rsinfo)
{
	ClusterHangDumpData data;
	int i;

	cluster_hang_get_dump_data(&data);

	emit_row(rsinfo, "hang", "hang_manager_enabled", fmt_bool(cluster_hang_manager_enabled));
	emit_row(rsinfo, "hang", "hang_dump_enabled", fmt_bool(cluster_hang_dump_enabled));
	emit_row(rsinfo, "hang", "hang_threshold_ms", fmt_int32(cluster_hang_threshold_ms));
	emit_row(rsinfo, "hang", "hang_sample_interval_ms", fmt_int32(cluster_hang_sample_interval_ms));
	emit_row(rsinfo, "hang", "hang_max_sampled", fmt_int32(cluster_hang_max_sampled));

	if (!data.available) {
		/* DIAG region not attached (e.g. cluster disabled): zeroed view. */
		emit_row(rsinfo, "hang", "hang_available", fmt_bool(false));
		return;
	}
	emit_row(rsinfo, "hang", "hang_available", fmt_bool(true));

	emit_row(rsinfo, "hang", "hang_sample_epoch", fmt_int64((int64)data.store.sample_epoch));
	emit_row(rsinfo, "hang", "hang_last_sample_at",
			 data.last_sample_at == 0 ? "(unset)"
									  : pstrdup(timestamptz_to_str(data.last_sample_at)));
	emit_row(rsinfo, "hang", "hang_last_dump_emitted_at",
			 data.last_dump_emitted_at == 0
				 ? "(unset)"
				 : pstrdup(timestamptz_to_str(data.last_dump_emitted_at)));
	emit_row(rsinfo, "hang", "hang_long_wait_count", fmt_int64(data.long_wait_count));
	emit_row(rsinfo, "hang", "hang_longest_wait_us", fmt_int64(data.longest_wait_us));
	emit_row(rsinfo, "hang", "hang_truncated", fmt_bool(data.store.truncated));
	emit_row(rsinfo, "hang", "hang_n_samples", fmt_int32(data.store.n_samples));

	/* Cumulative counters (D8). */
	emit_row(rsinfo, "hang", "hang_samples_taken", fmt_int64((int64)data.counters.samples_taken));
	emit_row(rsinfo, "hang", "hang_long_waits_seen",
			 fmt_int64((int64)data.counters.long_waits_seen));
	emit_row(rsinfo, "hang", "hang_dumps_emitted", fmt_int64((int64)data.counters.dumps_emitted));
	emit_row(rsinfo, "hang", "hang_incomplete_sample_count",
			 fmt_int64((int64)data.counters.incomplete_samples));
	emit_row(rsinfo, "hang", "hang_excluded_deadlock_count",
			 fmt_int64((int64)data.counters.excluded_deadlock));
	emit_row(rsinfo, "hang", "hang_excluded_idle_count",
			 fmt_int64((int64)data.counters.excluded_idle));
	emit_row(rsinfo, "hang", "hang_excluded_bgworker_count",
			 fmt_int64((int64)data.counters.excluded_bgworker));
	emit_row(rsinfo, "hang", "hang_proc_signal_dump_count",
			 fmt_int64((int64)data.counters.proc_signal_dumps));
	emit_row(rsinfo, "hang", "hang_error_count", fmt_int64((int64)data.counters.error_count));

	/*
	 * spec-5.11 D6 — spec-5.8 reader: aggregate cluster-wide deadlock context.
	 * The shipped 5.8 surface exposes deadlock confirmation only as these
	 * aggregate counters (not a per-proc confirmed-cycle flag), so per-sample
	 * in_confirmed_deadlock stays false; the live per-proc exclusion is
	 * forward to a 5.9 per-proc victim/confirmed signal (D0 re-ground).
	 */
	emit_row(rsinfo, "hang", "hang_deadlock_confirmed_count",
			 fmt_int64((int64)cluster_lmd_deadlock_confirmed_count_get()));
	emit_row(rsinfo, "hang", "hang_cycle_detected_count",
			 fmt_int64((int64)cluster_lmd_cycle_detected_count_get()));

	/*
	 * spec-5.12 D8 — Hang Manager disposition state + cumulative counters.
	 * Appended to the same `hang` category (no new category); the mode comes
	 * from the GUC, the rest from a consistent copy of the DIAG region.
	 */
	{
		ClusterHangResolveCounters rc;

		cluster_hang_resolve_get_counters(&rc);
		emit_row(rsinfo, "hang", "hang_resolution_mode",
				 cluster_hang_resolve_mode_str(cluster_hang_resolution_mode));
		emit_row(rsinfo, "hang", "hang_resolve_evaluations",
				 fmt_int64((int64)rc.resolve_evaluations));
		emit_row(rsinfo, "hang", "hang_victims_selected", fmt_int64((int64)rc.victims_selected));
		emit_row(rsinfo, "hang", "hang_soft_cancels_issued",
				 fmt_int64((int64)rc.soft_cancels_issued));
		emit_row(rsinfo, "hang", "hang_terminates_issued", fmt_int64((int64)rc.terminates_issued));
		emit_row(rsinfo, "hang", "hang_resolved_confirmed",
				 fmt_int64((int64)rc.resolved_confirmed));
		emit_row(rsinfo, "hang", "hang_resolution_failed", fmt_int64((int64)rc.resolution_failed));
		emit_row(rsinfo, "hang", "hang_hard_skipped", fmt_int64((int64)rc.hard_skipped));
		emit_row(rsinfo, "hang", "hang_non_actionable_skipped",
				 fmt_int64((int64)rc.non_actionable_skipped));
		emit_row(rsinfo, "hang", "hang_over_excluded", fmt_int64((int64)rc.over_excluded));
		emit_row(rsinfo, "hang", "hang_unprovable_root_skipped",
				 fmt_int64((int64)rc.unprovable_root_skipped));
		emit_row(rsinfo, "hang", "hang_aba_revalidate_failed",
				 fmt_int64((int64)rc.aba_revalidate_failed));
		emit_row(rsinfo, "hang", "hang_not_confirmed_yet", fmt_int64((int64)rc.not_confirmed_yet));
		emit_row(rsinfo, "hang", "hang_no_safe_victim", fmt_int64((int64)rc.no_safe_victim));
		emit_row(rsinfo, "hang", "hang_degraded_to_timeout",
				 fmt_int64((int64)rc.degraded_to_timeout));
		emit_row(rsinfo, "hang", "hang_advisory_recommendations",
				 fmt_int64((int64)rc.advisory_recommendations));
		emit_row(rsinfo, "hang", "hang_resolve_last_victim_pid", fmt_int32(rc.last_victim_pid));
		emit_row(rsinfo, "hang", "hang_resolve_last_action",
				 cluster_hang_action_tier_str(rc.last_action));
	}

	/* Per-row long-wait samples (real shmem backing). */
	for (i = 0; i < data.store.n_samples; i++) {
		const ClusterHangSampleSlot *s = &data.store.slots[i];

		emit_row(rsinfo, "hang", psprintf("hang_sample%d_pid", i), fmt_int32(s->pid));
		emit_row(rsinfo, "hang", psprintf("hang_sample%d_wait_event", i),
				 s->wait_event[0] ? pstrdup(s->wait_event) : "(none)");
		emit_row(rsinfo, "hang", psprintf("hang_sample%d_wait_ms", i),
				 fmt_int64(s->duration_us / 1000));
		emit_row(rsinfo, "hang", psprintf("hang_sample%d_duration_kind", i),
				 s->duration_kind == HANG_DUR_TRUE ? "true" : "approx");
		emit_row(rsinfo, "hang", psprintf("hang_sample%d_source", i),
				 cluster_hang_wait_source_str(s->source));
		emit_row(rsinfo, "hang", psprintf("hang_sample%d_quality", i),
				 cluster_hang_quality_str(s->quality));
		emit_row(rsinfo, "hang", psprintf("hang_sample%d_blocker_pid", i),
				 fmt_int32(s->blocker_pid));
		emit_row(rsinfo, "hang", psprintf("hang_sample%d_blocker_remote_node", i),
				 fmt_int32(s->blocker_remote_node));
		emit_row(rsinfo, "hang", psprintf("hang_sample%d_in_confirmed_deadlock", i),
				 fmt_bool(s->in_confirmed_deadlock));
	}
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
 *	Emits core routing rows plus entry lifecycle counters under
 *	category='grd':
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
 *	  - grd_entries_reclaimed_count: lifetime cold entry removes
 *	  - grd_reclaim_skipped_pinned_count: reclaim skipped because pin>0
 *	  - grd_pin_high_water:          max observed per-entry pin count
 *	  - grd_sweep_runs:              LMON reclaim sweep invocations
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
	emit_row(rsinfo, "grd", "grd_entries_reclaimed_count",
			 fmt_int64((int64)cluster_grd_entries_reclaimed_count()));
	emit_row(rsinfo, "grd", "grd_reclaim_skipped_pinned_count",
			 fmt_int64((int64)cluster_grd_reclaim_skipped_pinned_count()));
	emit_row(rsinfo, "grd", "grd_pin_high_water", fmt_int64((int64)cluster_grd_pin_high_water()));
	emit_row(rsinfo, "grd", "grd_sweep_runs", fmt_int64((int64)cluster_grd_sweep_runs()));

	emit_row(rsinfo, "grd", "grd_holders_full_count",
			 fmt_int64((int64)cluster_grd_holders_full_count()));
	emit_row(rsinfo, "grd", "grd_waiters_full_count",
			 fmt_int64((int64)cluster_grd_waiters_full_count()));
	emit_row(rsinfo, "grd", "grd_converts_full_count",
			 fmt_int64((int64)cluster_grd_converts_full_count()));
	/* spec-5.1b D9 — convert state-machine verdict counters (queue-full
	 * reuses grd_converts_full_count above). */
	emit_row(rsinfo, "grd", "grd_convert_granted_inplace_count",
			 fmt_int64((int64)cluster_grd_convert_granted_inplace_count()));
	emit_row(rsinfo, "grd", "grd_convert_enqueued_count",
			 fmt_int64((int64)cluster_grd_convert_enqueued_count()));
	emit_row(rsinfo, "grd", "grd_convert_illegal_count",
			 fmt_int64((int64)cluster_grd_convert_illegal_count()));
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
	/* spec-5.10 D7 — GES enqueue lock-starvation fairness counters. */
	emit_row(rsinfo, "grd", "grd_starvation_boost_count",
			 fmt_int64((int64)cluster_grd_starvation_boost_count()));
	emit_row(rsinfo, "grd", "grd_starvation_barrier_enqueued_count",
			 fmt_int64((int64)cluster_grd_starvation_barrier_enqueued_count()));
	emit_row(rsinfo, "grd", "grd_starvation_barrier_publish_fail_count",
			 fmt_int64((int64)cluster_grd_starvation_barrier_publish_fail_count()));
	emit_row(rsinfo, "grd", "grd_starvation_max_skip_observed",
			 fmt_int64((int64)cluster_grd_starvation_max_skip_observed()));
}

/* ============================================================
 * dump_grd_recovery -- spec-4.6 D5 failure-driven remaster
 *	observability (13 counters;  each has a t/249 acceptance leg).
 * ============================================================ */
static void
dump_grd_recovery(ReturnSetInfo *rsinfo)
{
	ClusterGrdRecoveryCounters c;

	cluster_grd_recovery_counters_snapshot(&c);
	emit_row(rsinfo, "grd_recovery", "remaster_started", fmt_int64((int64)c.remaster_started));
	emit_row(rsinfo, "grd_recovery", "remaster_done", fmt_int64((int64)c.remaster_done));
	emit_row(rsinfo, "grd_recovery", "remaster_failed", fmt_int64((int64)c.remaster_failed));
	emit_row(rsinfo, "grd_recovery", "shards_remastered", fmt_int64((int64)c.shards_remastered));
	emit_row(rsinfo, "grd_recovery", "holders_redeclared", fmt_int64((int64)c.holders_redeclared));
	emit_row(rsinfo, "grd_recovery", "holders_rebound", fmt_int64((int64)c.holders_rebound));
	emit_row(rsinfo, "grd_recovery", "waiters_requeued", fmt_int64((int64)c.waiters_requeued));
	emit_row(rsinfo, "grd_recovery", "converts_requeued", fmt_int64((int64)c.converts_requeued));
	emit_row(rsinfo, "grd_recovery", "stale_request_drop", fmt_int64((int64)c.stale_request_drop));
	emit_row(rsinfo, "grd_recovery", "rebuild_timeout", fmt_int64((int64)c.rebuild_timeout));
	emit_row(rsinfo, "grd_recovery", "block_path_failclosed",
			 fmt_int64((int64)c.block_path_failclosed));
	emit_row(rsinfo, "grd_recovery", "unaffected_holder_survived",
			 fmt_int64((int64)c.unaffected_holder_survived));
	emit_row(rsinfo, "grd_recovery", "stale_holder_swept", fmt_int64((int64)c.stale_holder_swept));
	/* spec-5.16 D5 — join-direction remaster counters (same grd_recovery
	 * category; no new dump category, §8 Q6-A). */
	emit_row(rsinfo, "grd_recovery", "join_remaster_started",
			 fmt_int64((int64)c.join_remaster_started));
	emit_row(rsinfo, "grd_recovery", "join_remaster_done", fmt_int64((int64)c.join_remaster_done));
	emit_row(rsinfo, "grd_recovery", "join_shards_remastered",
			 fmt_int64((int64)c.join_shards_remastered));
	emit_row(rsinfo, "grd_recovery", "join_block_views_rebuilt",
			 fmt_int64((int64)c.join_block_views_rebuilt));
	emit_row(rsinfo, "grd_recovery", "join_block_recovering_failclosed",
			 fmt_int64((int64)c.join_block_recovering_failclosed));
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
 *	Emits 47 rows under category='lmd' (spec-2.19 daemon state/counters +
 *	spec-2.22 graph/Tarjan + spec-2.23 probe + spec-2.24 cancel/cleanup +
 *	spec-5.8 D6 two-round-confirm/reconfig-gate + spec-5.8 Hardening v1.0.1
 *	FC1 member_incomplete_count + spec-5.8 D8 shmem REPORT-collector
 *	counters + spec-5.9 victim-policy/cancel-robustness)
 *	corresponding to the LMD observability surface (HC2 4-state
 *	semantic split via state column + 6 counters per §0 Q8).
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
	/* spec-5.8 D6 — 3 NEW coordinator two-round confirm + reconfig-gate counters. */
	emit_row(rsinfo, "lmd", "deadlock_confirmed_count",
			 fmt_int64((int64)cluster_lmd_deadlock_confirmed_count_get()));
	emit_row(rsinfo, "lmd", "confirm_unconfirmed_count",
			 fmt_int64((int64)cluster_lmd_confirm_unconfirmed_count_get()));
	emit_row(rsinfo, "lmd", "reconfig_discard_count",
			 fmt_int64((int64)cluster_lmd_reconfig_discard_count_get()));
	/* spec-5.8 Hardening v1.0.1 — FC1 acting gate (partial member set -> round
	 * discarded before Tarjan; never confirm / cancel). */
	emit_row(rsinfo, "lmd", "member_incomplete_count",
			 fmt_int64((int64)cluster_lmd_member_incomplete_count_get()));
	/* spec-5.9 D10 — 13 NEW victim-policy + cancel-robustness counters. */
	emit_row(rsinfo, "lmd", "victim_protected_skip_count",
			 fmt_int64((int64)cluster_lmd_victim_protected_skip_count_get()));
	emit_row(rsinfo, "lmd", "victim_repeat_avoided_count",
			 fmt_int64((int64)cluster_lmd_victim_repeat_avoided_count_get()));
	emit_row(rsinfo, "lmd", "cancel_token_installed_count",
			 fmt_int64((int64)cluster_lmd_cancel_token_installed_count_get()));
	emit_row(rsinfo, "lmd", "cancel_consumed_count",
			 fmt_int64((int64)cluster_lmd_cancel_consumed_count_get()));
	emit_row(rsinfo, "lmd", "cancel_stale_cleared_count",
			 fmt_int64((int64)cluster_lmd_cancel_stale_cleared_count_get()));
	emit_row(rsinfo, "lmd", "cancel_wait_stale_rejected_count",
			 fmt_int64((int64)cluster_lmd_cancel_wait_stale_rejected_count_get()));
	emit_row(rsinfo, "lmd", "cancel_ack_received_count",
			 fmt_int64((int64)cluster_lmd_cancel_ack_received_count_get()));
	emit_row(rsinfo, "lmd", "cancel_retransmit_count",
			 fmt_int64((int64)cluster_lmd_cancel_retransmit_count_get()));
	emit_row(rsinfo, "lmd", "cancel_escalated_alternate_count",
			 fmt_int64((int64)cluster_lmd_cancel_escalated_alternate_count_get()));
	emit_row(rsinfo, "lmd", "cancel_exhausted_timeout_count",
			 fmt_int64((int64)cluster_lmd_cancel_exhausted_timeout_count_get()));
	emit_row(rsinfo, "lmd", "cancel_no_safe_victim_count",
			 fmt_int64((int64)cluster_lmd_cancel_no_safe_victim_count_get()));
	emit_row(rsinfo, "lmd", "cleanup_orphan_edge_swept_count",
			 fmt_int64((int64)cluster_lmd_cleanup_orphan_edge_swept_count_get()));
	emit_row(rsinfo, "lmd", "reconfig_cancel_discarded_count",
			 fmt_int64((int64)cluster_lmd_reconfig_cancel_discarded_count_get()));
	/* spec-5.9 Hardening v1.0.1 (P1#1) — CANCEL_ACK victim/wait_seq mismatch drop. */
	emit_row(rsinfo, "lmd", "cancel_ack_mismatch_count",
			 fmt_int64((int64)cluster_lmd_cancel_ack_mismatch_count_get()));
	/* spec-5.8 D8 — 5 NEW shmem REPORT-collector counters (LMON->LMD hand-off). */
	emit_row(rsinfo, "lmd", "probe_report_enqueue_count",
			 fmt_int64((int64)cluster_lmd_probe_report_enqueue_count_get()));
	emit_row(rsinfo, "lmd", "probe_drop_stale_count",
			 fmt_int64((int64)cluster_lmd_probe_drop_stale_count_get()));
	emit_row(rsinfo, "lmd", "probe_drop_duplicate_count",
			 fmt_int64((int64)cluster_lmd_probe_drop_duplicate_count_get()));
	emit_row(rsinfo, "lmd", "probe_queue_full_count",
			 fmt_int64((int64)cluster_lmd_probe_queue_full_count_get()));
	emit_row(rsinfo, "lmd", "probe_partial_report_count",
			 fmt_int64((int64)cluster_lmd_probe_partial_report_count_get()));
}


/*
 * dump_advisory -- spec-5.5 D8 UL user lock (cross-node advisory) observability.
 *
 *	Emits 5 rows under category='advisory':
 *	  - advisory_globalize_count:       0->1 edges that entered the cluster path
 *	  - advisory_session_release_count: session-scoped holders drained
 *	  - advisory_try_grant_count:       NOWAIT conditional grants (S4 path)
 *	  - advisory_try_notavail_count:    NOWAIT returned false (conflict)
 *	  - advisory_failclosed_count:      mutual exclusion unprovable -> 53R80
 */
static void
dump_advisory(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "advisory", "advisory_globalize_count",
			 fmt_int64((int64)cluster_advisory_counter_read(CLUSTER_ADVISORY_GLOBALIZE)));
	emit_row(rsinfo, "advisory", "advisory_session_release_count",
			 fmt_int64((int64)cluster_advisory_counter_read(CLUSTER_ADVISORY_SESSION_RELEASE)));
	emit_row(rsinfo, "advisory", "advisory_try_grant_count",
			 fmt_int64((int64)cluster_advisory_counter_read(CLUSTER_ADVISORY_TRY_GRANT)));
	emit_row(rsinfo, "advisory", "advisory_try_notavail_count",
			 fmt_int64((int64)cluster_advisory_counter_read(CLUSTER_ADVISORY_TRY_NOTAVAIL)));
	emit_row(rsinfo, "advisory", "advisory_failclosed_count",
			 fmt_int64((int64)cluster_advisory_counter_read(CLUSTER_ADVISORY_FAILCLOSED)));
}

/*
 * dump_reconfig_touched -- spec-5.14 D6 touched_peers fail-stop observability.
 *
 *	Emits the touched_peers counters (in the existing ClusterReconfigState
 *	region) + this backend's own touched bitmap (low 64 nodes) under
 *	category='reconfig_touched'.
 */
static void
dump_reconfig_touched(ReturnSetInfo *rsinfo)
{
	char hexbuf[24];

	emit_row(rsinfo, "reconfig_touched", "abort_count",
			 fmt_int64((int64)cluster_reconfig_get_touched_abort_count()));
	emit_row(rsinfo, "reconfig_touched", "stamp_count",
			 fmt_int64((int64)cluster_reconfig_get_touched_stamp_count()));
	emit_row(rsinfo, "reconfig_touched", "stamp_ges",
			 fmt_int64((int64)cluster_reconfig_get_touched_stamp_by_kind(CLUSTER_TOUCH_GES_LOCK)));
	emit_row(rsinfo, "reconfig_touched", "stamp_gcs_block",
			 fmt_int64((int64)cluster_reconfig_get_touched_stamp_by_kind(CLUSTER_TOUCH_GCS_BLOCK)));
	emit_row(rsinfo, "reconfig_touched", "stamp_scn",
			 fmt_int64((int64)cluster_reconfig_get_touched_stamp_by_kind(CLUSTER_TOUCH_SCN)));
	emit_row(
		rsinfo, "reconfig_touched", "stamp_vis",
		fmt_int64((int64)cluster_reconfig_get_touched_stamp_by_kind(CLUSTER_TOUCH_VISIBILITY)));
	emit_row(rsinfo, "reconfig_touched", "stamp_sinval",
			 fmt_int64((int64)cluster_reconfig_get_touched_stamp_by_kind(CLUSTER_TOUCH_SINVAL)));
	emit_row(rsinfo, "reconfig_touched", "clean_leave_rejected",
			 fmt_int64((int64)cluster_reconfig_get_clean_leave_rejected_count()));

	cluster_touched_peers_self_hex(hexbuf, sizeof(hexbuf));
	emit_row(rsinfo, "reconfig_touched", "self_touched_hex", hexbuf);
}

/*
 * dump_reconfig_join -- spec-5.15 D6 online-join observability.  5 lifetime
 * counters under category='reconfig_join'.
 */
static void
dump_reconfig_join(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "reconfig_join", "join_pending_count",
			 fmt_int64((int64)cluster_reconfig_get_join_pending_count()));
	emit_row(rsinfo, "reconfig_join", "join_apply_count",
			 fmt_int64((int64)cluster_reconfig_get_join_apply_count()));
	emit_row(rsinfo, "reconfig_join", "join_reject_count",
			 fmt_int64((int64)cluster_reconfig_get_join_reject_count()));
	emit_row(rsinfo, "reconfig_join", "join_timeout_count",
			 fmt_int64((int64)cluster_reconfig_get_join_timeout_count()));
	emit_row(rsinfo, "reconfig_join", "clean_departed_cleared_count",
			 fmt_int64((int64)cluster_reconfig_get_clean_departed_cleared_count()));
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
	/* spec-5.2 D4/D6:  cross-node TX enqueue completion-wait counters. */
	emit_row(rsinfo, "ges", "tx_enqueue_wait_count",
			 fmt_int64((int64)cluster_txw_get_wait_count()));
	emit_row(rsinfo, "ges", "tx_enqueue_wakeup_count",
			 fmt_int64((int64)cluster_txw_get_wakeup_count()));
	emit_row(rsinfo, "ges", "tx_enqueue_timeout_count",
			 fmt_int64((int64)cluster_txw_get_timeout_count()));
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
	/* spec-5.2 D1:  relsize SMGR-inval broadcasts emitted (source side). */
	emit_row(rsinfo, "shared_fs", "smgr_inval_bcast_sent_count",
			 fmt_int64((int64)cluster_smgr_get_inval_bcast_sent_count()));
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
	/* PGRAC: 3 = the real PCM lock modes (N/S/X).  spec-5.2 §3.5 D11 added a
	 * 4th PcmState value, PCM_STATE_READ_IMAGE, but it is a transient
	 * BufferDesc.pcm_state marker (deferred-writer read-image), NOT a PCM lock
	 * mode and never on the wire/GRD, so it is intentionally excluded here. */
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

	/* PGRAC: spec-6.14a D5 — local (b)-leg fail-closed counter (pcm row,
	 * emitted here so the category groups stay contiguous). */
	emit_row(rsinfo, "pcm", "local_s_revoke_nonholder_failclosed_count",
			 fmt_int64((int64)cluster_pcm_get_local_s_revoke_nonholder_failclosed_count()));
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
/* ============================================================
 * spec-5.6 Dc4: CF (shared control-file authority) observability.
 *	Exposes the five cluster_cf_stats counters under category 'cf' so the
 *	cluster_tap CF tests can assert CF X was taken (serialization proof),
 *	fail-closed events, single-node-authority windows, and .bak fallbacks.
 * ============================================================ */
static void
dump_cf(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "cf", "cf_x_acquire",
			 fmt_int64((int64)cluster_cf_counter_read(CLUSTER_CF_X_ACQUIRE)));
	emit_row(rsinfo, "cf", "cf_s_acquire",
			 fmt_int64((int64)cluster_cf_counter_read(CLUSTER_CF_S_ACQUIRE)));
	emit_row(rsinfo, "cf", "cf_failclosed",
			 fmt_int64((int64)cluster_cf_counter_read(CLUSTER_CF_FAILCLOSED)));
	emit_row(rsinfo, "cf", "cf_single_node_authority",
			 fmt_int64((int64)cluster_cf_counter_read(CLUSTER_CF_SINGLE_NODE_AUTHORITY)));
	emit_row(rsinfo, "cf", "cf_bak_fallback",
			 fmt_int64((int64)cluster_cf_counter_read(CLUSTER_CF_BAK_FALLBACK)));
}

static void
dump_smart_fusion(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "smart_fusion", "dep_install_count",
			 fmt_int64((int64)cluster_sf_dep_install_count()));
	emit_row(rsinfo, "smart_fusion", "dep_touch_count",
			 fmt_int64((int64)cluster_sf_dep_touch_count()));
	emit_row(rsinfo, "smart_fusion", "dbwr_brake_count",
			 fmt_int64((int64)cluster_sf_dep_dbwr_brake_count()));
	emit_row(rsinfo, "smart_fusion", "commit_brake_count",
			 fmt_int64((int64)cluster_sf_dep_commit_brake_count()));
	emit_row(rsinfo, "smart_fusion", "commit_brake_wait_us",
			 fmt_int64((int64)cluster_sf_dep_commit_brake_wait_us()));
	emit_row(rsinfo, "smart_fusion", "origin_suspect_count",
			 fmt_int64((int64)cluster_sf_dep_origin_suspect_count()));
	emit_row(rsinfo, "smart_fusion", "dep_lost_failclosed_count",
			 fmt_int64((int64)cluster_sf_dep_lost_failclosed_count()));
	emit_row(rsinfo, "smart_fusion", "retry_failclosed_count",
			 fmt_int64((int64)cluster_sf_dep_retry_failclosed_count()));
}

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
	/* spec-6.13 D8: RDMA tier3/direct-land copy path observability. */
	emit_row(rsinfo, "gcs", "scratch_copy_count",
			 fmt_int64((int64)cluster_gcs_get_scratch_copy_count()));
	emit_row(rsinfo, "gcs", "live_sge_send_count",
			 fmt_int64((int64)cluster_gcs_get_live_sge_send_count()));
	emit_row(rsinfo, "gcs", "live_sge_fallback_count",
			 fmt_int64((int64)cluster_gcs_get_live_sge_fallback_count()));
	emit_row(rsinfo, "gcs", "direct_install_count",
			 fmt_int64((int64)cluster_gcs_get_direct_install_count()));
	emit_row(rsinfo, "gcs", "direct_install_abort_count",
			 fmt_int64((int64)cluster_gcs_get_direct_install_abort_count()));
	emit_row(rsinfo, "gcs", "install_copy_count",
			 fmt_int64((int64)cluster_gcs_get_install_copy_count()));

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

	/* PGRAC: spec-6.14a D5 — 3 NEW gcs counter rows for the X-vs-S arms
	 * (the companion pcm-category row lives in dump_pcm). */
	emit_row(rsinfo, "gcs", "local_s_upgrade_grant_count",
			 fmt_int64((int64)cluster_gcs_get_local_s_upgrade_grant_count()));
	emit_row(rsinfo, "gcs", "x_vs_s_nonholder_grant_count",
			 fmt_int64((int64)cluster_gcs_get_x_vs_s_nonholder_grant_count()));
	emit_row(rsinfo, "gcs", "x_vs_s_no_carrier_denied_count",
			 fmt_int64((int64)cluster_gcs_get_x_vs_s_no_carrier_denied_count()));

	/* PGRAC: spec-2.37 D12 — 4 NEW counter rows for PI watermark + lost-write. */
	emit_row(rsinfo, "gcs", "pi_watermark_advance_count",
			 fmt_int64((int64)cluster_gcs_get_pi_watermark_advance_count()));
	emit_row(rsinfo, "gcs", "pi_watermark_retire_count",
			 fmt_int64((int64)cluster_gcs_get_pi_watermark_retire_count()));
	emit_row(rsinfo, "gcs", "lost_write_detected_count",
			 fmt_int64((int64)cluster_gcs_get_lost_write_detected_count()));
	emit_row(rsinfo, "gcs", "lost_write_avoid_count",
			 fmt_int64((int64)cluster_gcs_get_lost_write_avoid_count()));
	/* PGRAC: spec-2.41 D7 — SCN detector branch breakdown (§2.6). */
	emit_row(rsinfo, "gcs", "lost_write_invalidscn_failclosed_count",
			 fmt_int64((int64)cluster_gcs_get_lost_write_invalidscn_failclosed_count()));
	emit_row(rsinfo, "gcs", "lost_write_not_scn_tracked_skip_count",
			 fmt_int64((int64)cluster_gcs_get_lost_write_not_scn_tracked_skip_count()));
	/* PGRAC: spec-5.2 D2 — X-holder read-image ship counter. */
	emit_row(rsinfo, "gcs", "cf_xheld_read_ship_count",
			 fmt_int64((int64)cluster_gcs_get_cf_xheld_read_ship_count()));
	/* PGRAC: spec-5.2 D11 — writer-transfer-revoke ship counters (path A/B). */
	emit_row(rsinfo, "gcs", "block_x_transfer_ship_count",
			 fmt_int64((int64)cluster_gcs_get_block_x_transfer_ship_count()));
	emit_row(rsinfo, "gcs", "block_x_self_ship_count",
			 fmt_int64((int64)cluster_gcs_get_block_x_self_ship_count()));
	/* PGRAC: spec-5.2a D6 — 5 NEW clean-page X-transfer enabler counter rows. */
	emit_row(rsinfo, "gcs", "clean_page_xfer_count",
			 fmt_int64((int64)cluster_gcs_get_clean_page_xfer_count()));
	emit_row(rsinfo, "gcs", "clean_page_xfer_storage_fallback_count",
			 fmt_int64((int64)cluster_gcs_get_clean_page_xfer_storage_fallback_count()));
	emit_row(rsinfo, "gcs", "clean_page_xfer_fail_closed_count",
			 fmt_int64((int64)cluster_gcs_get_clean_page_xfer_fail_closed_count()));
	emit_row(rsinfo, "gcs", "clean_page_xfer_stale_holder_recover_count",
			 fmt_int64((int64)cluster_gcs_get_clean_page_xfer_stale_holder_recover_count()));
	emit_row(rsinfo, "gcs", "clean_page_xfer_third_party_denied_count",
			 fmt_int64((int64)cluster_gcs_get_clean_page_xfer_third_party_denied_count()));

	/*
	 * PGRAC: spec-5.4 D9 — 6 SQ sequence counter rows (v2.0 Q2-B option B).
	 *	refill / refill_wait / cycle_rejected fire on the cluster nextval path;
	 *	dup_guard_fail fires when the activation gate fails closed on a
	 *	non-shared user sequence; page_writeback fires when a refill makes the
	 *	shared-page boundary durable + storage-visible.  failover_fail_closed is
	 *	RESERVED (Q2-B's shared page handles failover naturally; a Stage-6
	 *	failover-continuity spec will consume it) — mirrors the spec-5.2a
	 *	storage_fallback / stale_holder_recover reserved-counter precedent.
	 */
	emit_row(rsinfo, "sequence", "sq_refill_count", fmt_int64((int64)cluster_sq_refill_count()));
	emit_row(rsinfo, "sequence", "sq_refill_wait_count",
			 fmt_int64((int64)cluster_sq_refill_wait_count()));
	emit_row(rsinfo, "sequence", "sq_dup_guard_fail_count",
			 fmt_int64((int64)cluster_sq_dup_guard_fail_count()));
	emit_row(rsinfo, "sequence", "sq_failover_fail_closed_count",
			 fmt_int64((int64)cluster_sq_failover_fail_closed_count()));
	emit_row(rsinfo, "sequence", "sq_page_writeback_count",
			 fmt_int64((int64)cluster_sq_page_writeback_count()));
	emit_row(rsinfo, "sequence", "sq_cycle_rejected_count",
			 fmt_int64((int64)cluster_sq_cycle_rejected_count()));

	/* PGRAC: spec-4.7 D6 — 8 NEW counter rows for GCS/PCM warm recovery. */
	emit_row(rsinfo, "gcs_recovery", "block_resources_recovering",
			 fmt_int64((int64)cluster_gcs_get_recovery_block_resources_recovering()));
	emit_row(rsinfo, "gcs_recovery", "buffers_redeclared",
			 fmt_int64((int64)cluster_gcs_get_recovery_buffers_redeclared()));
	emit_row(rsinfo, "gcs_recovery", "block_state_rebuilt",
			 fmt_int64((int64)cluster_gcs_get_recovery_block_state_rebuilt()));
	emit_row(rsinfo, "gcs_recovery", "redo_boundary_waits",
			 fmt_int64((int64)cluster_gcs_get_recovery_redo_boundary_waits()));
	emit_row(rsinfo, "gcs_recovery", "redo_boundary_reached",
			 fmt_int64((int64)cluster_gcs_get_recovery_redo_boundary_reached()));
	/* PGRAC: spec-2.41 D7 — redo-coverage serve-gate observability (§2.8 guard).
	 * required_lsn_zero_count must stay 0 except real cold blocks; a spike means
	 * the SCN migration wrongly zeroed the LSN watermark feeding the serve-gate. */
	emit_row(rsinfo, "gcs_recovery", "redo_coverage_required_lsn_zero_count",
			 fmt_int64((int64)cluster_gcs_get_redo_coverage_required_lsn_zero_count()));
	emit_row(rsinfo, "gcs_recovery", "redo_coverage_gate_block_count",
			 fmt_int64((int64)cluster_gcs_get_redo_coverage_gate_block_count()));
	emit_row(rsinfo, "gcs_recovery", "stale_block_drop",
			 fmt_int64((int64)cluster_gcs_get_recovery_stale_block_drop()));
	emit_row(rsinfo, "gcs_recovery", "ambiguous_owner_failclosed",
			 fmt_int64((int64)cluster_gcs_get_recovery_ambiguous_owner_failclosed()));
	emit_row(rsinfo, "gcs_recovery", "before_boundary_failclosed",
			 fmt_int64((int64)cluster_gcs_get_recovery_before_boundary_failclosed()));

	/* PGRAC: spec-4.8 — 8 tt_recovery counter rows (undo/TT recovery verdicts). */
	emit_row(rsinfo, "tt_recovery", "active_slots_resolved_aborted",
			 fmt_int64((int64)cluster_tt_recovery_active_resolved_aborted_count()));
	emit_row(rsinfo, "tt_recovery", "remote_active_failclosed",
			 fmt_int64((int64)cluster_tt_recovery_remote_active_failclosed_count()));
	emit_row(rsinfo, "tt_recovery", "wrap_generation_disambiguated",
			 fmt_int64((int64)cluster_tt_recovery_wrap_generation_disambiguated_count()));
	emit_row(rsinfo, "tt_recovery", "recycled_liveness_relaxed",
			 fmt_int64((int64)cluster_tt_recovery_recycled_liveness_relaxed_count()));
	emit_row(rsinfo, "tt_recovery", "scn_highwater_recovered",
			 fmt_int64((int64)cluster_tt_recovery_scn_highwater_recovered_count()));
	emit_row(rsinfo, "tt_recovery", "recovery_verdict_failclosed",
			 fmt_int64((int64)cluster_tt_recovery_recovery_verdict_failclosed_count()));
	emit_row(rsinfo, "tt_recovery", "heap_tuples_physically_reverted",
			 fmt_int64((int64)cluster_tt_recovery_heap_tuples_physically_reverted_count()));
	emit_row(rsinfo, "tt_recovery", "undo_revert_failclosed",
			 fmt_int64((int64)cluster_tt_recovery_undo_revert_failclosed_count()));

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
	/* spec-5.2 D1 (G3):  relsize SMGR-inval apply barrier (peers re-stat). */
	emit_row(rsinfo, "sinval", "smgr_inval_applied_count",
			 fmt_int64((int64)cluster_sinval_get_smgr_inval_applied_count()));

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
 *	Emits 53 rows under category='undo': 5 record-level allocator counters
 *	(spec-3.7) + 4 segment-lifecycle counters (spec-3.8) + 3 commit-fsync +
 *	4 smgr counters (the latter 7 added by the perf-merge undo
 *	instrumentation) + 5 durable TT slot counters (spec-3.11 D8) + 5 retention
 *	counters (spec-3.12 D5: horizon gauge / tt_slot_retain_skip /
 *	segment_retain_skip / retention_recycle / tt_retention_rollover) +
 *	9 terminal authority counters (spec-6.2) +
 *	6 cleaner/reuse counters (spec-3.13 D6) +
 *	4 checkpoint-writeback boundary counters (spec-4.8ab D7:
 *	undo_buf_held_wal / undo_buf_held_evidence / undo_buf_boundary_violations /
 *	undo_buf_remote_evidence_holds).  Backs
 *	cluster_tap t/213 + t/214 + t/219 L2 + t/220 + t/270 verification + perf
 *	class 7 baseline tracking.
 */
/*
 * dbstate_text -- short token for a ControlFile DBState value
 *	(spec-4.3 plan observability; underscore form for greppability).
 */
static const char *
dbstate_text(uint32 dbstate)
{
	switch ((DBState)dbstate) {
	case DB_STARTUP:
		return "starting_up";
	case DB_SHUTDOWNED:
		return "shutdowned";
	case DB_SHUTDOWNED_IN_RECOVERY:
		return "shutdowned_in_recovery";
	case DB_SHUTDOWNING:
		return "shutting_down";
	case DB_IN_CRASH_RECOVERY:
		return "in_crash_recovery";
	case DB_IN_ARCHIVE_RECOVERY:
		return "in_archive_recovery";
	case DB_IN_PRODUCTION:
		return "in_production";
	default:
		return "unknown";
	}
}

/*
 * verdict_csv -- comma list of threads whose plan verdict matches;
 *	"-" when none.  Palloc'd (SRF per-call context).
 */
static const char *
verdict_csv(const ClusterRecoveryPlan *plan, ClusterRecoveryThreadVerdict want)
{
	StringInfoData buf;
	uint16 tid;

	initStringInfo(&buf);
	for (tid = 1; tid <= CLUSTER_RECOVERY_PLAN_THREADS; tid++) {
		if (plan->verdict[tid] == (uint8)want)
			appendStringInfo(&buf, "%s%u", buf.len > 0 ? "," : "", (unsigned)tid);
	}
	if (buf.len == 0)
		appendStringInfoString(&buf, "-");
	return buf.data; /* tuplestore copies; same lifetime as fmt_* helpers */
}

/*
 * dump_recovery -- spec-3.16 D5 recovery observability (4 rows) +
 *	spec-4.10 D6 single-block recovery (2 rows) + spec-4.11 D5 online
 *	thread recovery (4 rows) + spec-4.3 D5 recovery plan surface (13
 *	rows) + spec-4.4 D6 worker pool surface (8 rows) + spec-4.5a D11
 *	merged-replay / remote-read surface (8 rows; 39 total).
 */
static void
dump_recovery(ReturnSetInfo *rsinfo)
{
	ClusterRecoveryPlan plan;
	bool have_plan;

	emit_row(rsinfo, "recovery", "recovery_undo_redo_applies",
			 fmt_int64((int64)cluster_vis_get_recovery_undo_redo_applies()));
	emit_row(rsinfo, "recovery", "recovery_undo_redo_skips",
			 fmt_int64((int64)cluster_vis_get_recovery_undo_redo_skips()));
	emit_row(rsinfo, "recovery", "recovery_2pc_standby_rebuilds",
			 fmt_int64((int64)cluster_vis_get_recovery_2pc_standby_rebuilds()));
	emit_row(rsinfo, "recovery", "recovery_overlay_rebuild_count",
			 fmt_int64((int64)cluster_vis_get_recovery_overlay_rebuild_count()));

	/* spec-4.10 D6: online single-block recovery outcomes. */
	emit_row(rsinfo, "recovery", "block_recovery_blocks_recovered",
			 fmt_int64((int64)cluster_block_recovery_get_blocks_recovered()));
	emit_row(rsinfo, "recovery", "block_recovery_failclosed",
			 fmt_int64((int64)cluster_block_recovery_get_failclosed()));

	/* spec-4.11 D5: online thread recovery (#84) outcomes. */
	emit_row(rsinfo, "recovery", "thread_recovery_state", cluster_thread_recovery_state_name());
	emit_row(rsinfo, "recovery", "thread_recovery_threads_recovered",
			 fmt_int64((int64)cluster_thread_recovery_get_threads_recovered()));
	emit_row(rsinfo, "recovery", "thread_recovery_replay_failclosed",
			 fmt_int64((int64)cluster_thread_recovery_get_replay_failclosed()));
	emit_row(rsinfo, "recovery", "thread_recovery_recovered_through_lsn",
			 fmt_uint64_hex((uint64)cluster_thread_recovery_get_recovered_through()));

	/* spec-4.3 D5: recovery plan (observational; '-' before a plan). */
	have_plan = cluster_recovery_plan_snapshot(&plan);
	emit_row(rsinfo, "recovery", "plan_state",
			 !have_plan ? "none" : (plan.failed ? "failed" : "generated"));
	emit_row(rsinfo, "recovery", "plan_generated_at",
			 have_plan ? fmt_timestamptz((TimestampTz)plan.generated_at) : "-");
	emit_row(rsinfo, "recovery", "plan_own_thread",
			 have_plan ? fmt_int64((int64)plan.own_thread) : "-");
	emit_row(rsinfo, "recovery", "plan_threads_scanned",
			 have_plan ? fmt_int64((int64)plan.threads_scanned) : "-");
	emit_row(rsinfo, "recovery", "plan_crashed_candidates",
			 have_plan ? verdict_csv(&plan, CLUSTER_RECOVERY_THREAD_CRASHED_CANDIDATE) : "-");
	emit_row(rsinfo, "recovery", "plan_n_clean", have_plan ? fmt_int64((int64)plan.n_clean) : "-");
	emit_row(rsinfo, "recovery", "plan_n_empty", have_plan ? fmt_int64((int64)plan.n_empty) : "-");
	emit_row(rsinfo, "recovery", "plan_n_crashed_candidate",
			 have_plan ? fmt_int64((int64)plan.n_crashed_candidate) : "-");
	emit_row(rsinfo, "recovery", "plan_n_alive", have_plan ? fmt_int64((int64)plan.n_alive) : "-");
	emit_row(rsinfo, "recovery", "plan_n_unknown",
			 have_plan ? fmt_int64((int64)plan.n_unknown) : "-");
	emit_row(rsinfo, "recovery", "plan_unknown_threads",
			 have_plan ? verdict_csv(&plan, CLUSTER_RECOVERY_THREAD_UNKNOWN) : "-");
	emit_row(rsinfo, "recovery", "plan_dbstate_at_startup",
			 have_plan ? dbstate_text(plan.dbstate_at_startup) : "-");
	emit_row(rsinfo, "recovery", "plan_local_recovery_needed",
			 have_plan ? fmt_bool(plan.local_recovery_needed) : "-");

	/* spec-4.4 D6: worker pool surface (8 rows; 17 -> 25).  Counts are
	 * derived from slot_state[] on read (round-1 P1-3: no concurrent
	 * counters in shmem). */
	{
		ClusterRecoveryWorkerPool wp;
		bool have_pool = cluster_recovery_worker_pool_snapshot(&wp);
		int started = 0;
		int done = 0;
		int failed = 0;
		int slot;
		StringInfoData okcsv;
		StringInfoData badcsv;
		uint16 tid;
		const char *pool_state = "idle";

		if (have_pool) {
			for (slot = 0; slot < CLUSTER_RECOVERY_WORKER_MAX_SLOTS; slot++) {
				switch (pg_atomic_read_u32(&wp.slot_state[slot])) {
				case CLUSTER_RECOVERY_WORKER_RUNNING:
					started++;
					break;
				case CLUSTER_RECOVERY_WORKER_DONE:
					started++;
					done++;
					break;
				case CLUSTER_RECOVERY_WORKER_FAILED:
					started++;
					failed++;
					break;
				case CLUSTER_RECOVERY_WORKER_SPAWN_FAILED:
					/* round-2 P2: never existed; failed but not started */
					failed++;
					break;
				default:
					break;
				}
			}
			if (wp.workers_requested > 0) {
				if (done + failed < (int)wp.workers_requested)
					pool_state = "launched";
				else
					pool_state = (failed > 0) ? "partial-failed" : "done";
			}
		}

		initStringInfo(&okcsv);
		initStringInfo(&badcsv);
		for (tid = 1; have_pool && tid <= CLUSTER_WAL_STATE_SLOT_COUNT; tid++) {
			if (wp.stream_verdict[tid] == (uint8)CLUSTER_RECOVERY_STREAM_OK)
				appendStringInfo(&okcsv, "%s%u", okcsv.len > 0 ? "," : "", (unsigned)tid);
			else if (wp.stream_verdict[tid] == (uint8)CLUSTER_RECOVERY_STREAM_SUSPECT
					 || wp.stream_verdict[tid] == (uint8)CLUSTER_RECOVERY_STREAM_UNREADABLE)
				appendStringInfo(&badcsv, "%s%u", badcsv.len > 0 ? "," : "", (unsigned)tid);
		}
		if (okcsv.len == 0)
			appendStringInfoString(&okcsv, "-");
		if (badcsv.len == 0)
			appendStringInfoString(&badcsv, "-");

		emit_row(rsinfo, "recovery", "worker_pool_state", have_pool ? pool_state : "-");
		emit_row(rsinfo, "recovery", "worker_generation",
				 have_pool ? fmt_int64((int64)wp.generation) : "-");
		emit_row(rsinfo, "recovery", "workers_requested",
				 have_pool ? fmt_int64((int64)wp.workers_requested) : "-");
		emit_row(rsinfo, "recovery", "workers_started", have_pool ? fmt_int64(started) : "-");
		emit_row(rsinfo, "recovery", "workers_done", have_pool ? fmt_int64(done) : "-");
		emit_row(rsinfo, "recovery", "workers_failed", have_pool ? fmt_int64(failed) : "-");
		emit_row(rsinfo, "recovery", "stream_ok_threads", okcsv.data);
		emit_row(rsinfo, "recovery", "stream_suspect_or_unreadable_threads", badcsv.data);
	}

	/* spec-4.5a D11: merged-replay + remote-read observability (8 rows).
	 * materialized_remote_instances derives from the persistent wal_state
	 * registry (merge_recovered_lsn > 0), the same authority the remote
	 * read gates consult -- no separate bookkeeping to drift. */
	{
		StringInfoData matcsv;
		int origin;

		initStringInfo(&matcsv);
		for (origin = 0; origin < CLUSTER_WAL_STATE_SLOT_COUNT; origin++) {
			if (cluster_merged_instance_is_materialized(origin))
				appendStringInfo(&matcsv, "%s%d", matcsv.len > 0 ? "," : "", origin);
		}
		emit_row(rsinfo, "recovery", "merged_records_applied",
				 fmt_int64((int64)cluster_vis_get_merged_records_applied()));
		emit_row(rsinfo, "recovery", "merged_skipped_local",
				 fmt_int64((int64)cluster_vis_get_merged_skipped_local()));
		emit_row(rsinfo, "recovery", "merged_own_bound_skips",
				 fmt_int64((int64)cluster_vis_get_merged_own_bound_skips()));
		emit_row(rsinfo, "recovery", "materialized_remote_instances",
				 matcsv.len > 0 ? matcsv.data : "-");
		emit_row(rsinfo, "recovery", "remote_uba_resolved",
				 fmt_int64((int64)cluster_vis_get_remote_uba_resolved()));
		emit_row(rsinfo, "recovery", "remote_outcome_committed",
				 fmt_int64((int64)cluster_remote_xact_diverted_commit_count()));
		emit_row(rsinfo, "recovery", "remote_outcome_aborted",
				 fmt_int64((int64)cluster_remote_xact_diverted_abort_count()));
		emit_row(rsinfo, "recovery", "remote_authority_53ra",
				 fmt_int64((int64)cluster_remote_xact_outcome_indoubt_count()));
		pfree(matcsv.data);
	}
}


/*
 * dump_tt_2pc -- spec-3.15 D9 two-phase commit observability (6 rows).
 */
static void
dump_tt_2pc(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "tt_2pc", "twopc_prepare_records",
			 fmt_int64((int64)cluster_vis_get_twopc_prepare_records()));
	emit_row(rsinfo, "tt_2pc", "twopc_prepare_undo_flushes",
			 fmt_int64((int64)cluster_vis_get_twopc_prepare_undo_flushes()));
	emit_row(rsinfo, "tt_2pc", "twopc_postprepare_transfers",
			 fmt_int64((int64)cluster_vis_get_twopc_postprepare_transfers()));
	emit_row(rsinfo, "tt_2pc", "twopc_prefinish_commits",
			 fmt_int64((int64)cluster_vis_get_twopc_prefinish_commits()));
	emit_row(rsinfo, "tt_2pc", "twopc_prefinish_aborts",
			 fmt_int64((int64)cluster_vis_get_twopc_prefinish_aborts()));
	emit_row(rsinfo, "tt_2pc", "twopc_recover_rebinds",
			 fmt_int64((int64)cluster_vis_get_twopc_recover_rebinds()));
}


/*
 * dump_visibility -- spec-3.14 D8 HeapTupleSatisfies* variant fork
 * observability (6 counters under category='visibility').
 */
static void
dump_visibility(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "visibility", "vis_update_fork_count",
			 fmt_int64((int64)cluster_vis_get_vis_update_fork_count()));
	emit_row(rsinfo, "visibility", "vis_dirty_fork_count",
			 fmt_int64((int64)cluster_vis_get_vis_dirty_fork_count()));
	emit_row(rsinfo, "visibility", "vis_selftoast_fork_count",
			 fmt_int64((int64)cluster_vis_get_vis_selftoast_fork_count()));
	emit_row(rsinfo, "visibility", "vis_conflict_failclosed_count",
			 fmt_int64((int64)cluster_vis_get_vis_conflict_failclosed_count()));
	emit_row(rsinfo, "visibility", "prune_remote_keep_count",
			 fmt_int64((int64)cluster_vis_get_prune_remote_keep_count()));
	emit_row(rsinfo, "visibility", "vis_variant_unknown_failclosed_count",
			 fmt_int64((int64)cluster_vis_get_vis_variant_unknown_failclosed_count()));
}


static void
dump_undo(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "undo", "record_alloc_count",
			 fmt_int64((int64)cluster_undo_record_alloc_count()));
	emit_row(rsinfo, "undo", "segment_claim_count",
			 fmt_int64((int64)cluster_undo_segment_claim_count()));
	/* spec-3.18 D7: per-txn extent claim (D3) + undo buffer pool (D1/D2b). */
	emit_row(rsinfo, "undo", "extent_claim_count",
			 fmt_int64((int64)cluster_undo_extent_claim_count()));
	emit_row(rsinfo, "undo", "undo_buf_hit_count",
			 fmt_int64((int64)cluster_undo_buf_get_hit_count()));
	emit_row(rsinfo, "undo", "undo_buf_miss_count",
			 fmt_int64((int64)cluster_undo_buf_get_miss_count()));
	emit_row(rsinfo, "undo", "undo_buf_writeback_count",
			 fmt_int64((int64)cluster_undo_buf_get_writeback_count()));
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
	/* spec-4.12a D5: record-segment ACTIVE->COMMITTED drain observability.
	 *   record_segments_committed          : record segments drained to COMMITTED
	 *                                         (the leak fix advancing them).
	 *   record_seg_commit_skipped_inflight  : drain attempts a hard gate retained
	 *                                         (proves the 8.A guard fires). */
	emit_row(rsinfo, "undo", "record_segments_committed",
			 fmt_int64((int64)cluster_undo_record_segments_committed_count()));
	emit_row(rsinfo, "undo", "record_seg_commit_skipped_inflight",
			 fmt_int64((int64)cluster_undo_record_seg_commit_skipped_inflight_count()));
	/* spec-4.12a Hardening v1.0.1: residual extents dropped by locked
	 * revalidation (would-be stale reuse of a rolled-away/sealed segment). */
	emit_row(rsinfo, "undo", "record_seg_residual_revalidate_drops",
			 fmt_int64((int64)cluster_undo_record_seg_residual_revalidate_drop_count()));
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
	/* spec-3.22: ungated COMMITTED recycles this incarnation (0 = the xmax
	 * 0-match invisible shortcut is sound; non-zero disqualifies it). */
	emit_row(rsinfo, "undo", "retention_off_recycle_count",
			 fmt_int64((int64)cluster_tt_slot_retention_off_recycle_count()));
	emit_row(rsinfo, "undo", "tt_retention_rollover_count",
			 fmt_int64((int64)cluster_undo_tt_retention_rollover_count()));

	/* spec-6.2: Smart Fusion terminal-authority substrate counters. */
	emit_row(rsinfo, "undo", "terminal_authority_check_count",
			 fmt_int64((int64)cluster_terminal_authority_check_count()));
	emit_row(rsinfo, "undo", "terminal_authority_ok_count",
			 fmt_int64((int64)cluster_terminal_authority_ok_count()));
	emit_row(rsinfo, "undo", "terminal_authority_failclosed_count",
			 fmt_int64((int64)cluster_terminal_authority_failclosed_count()));
	emit_row(rsinfo, "undo", "terminal_authority_epoch_failclosed_count",
			 fmt_int64((int64)cluster_terminal_authority_epoch_failclosed_count()));
	emit_row(rsinfo, "undo", "terminal_authority_ownership_failclosed_count",
			 fmt_int64((int64)cluster_terminal_authority_ownership_failclosed_count()));
	emit_row(rsinfo, "undo", "terminal_authority_unknown_failclosed_count",
			 fmt_int64((int64)cluster_terminal_authority_unknown_failclosed_count()));
	emit_row(rsinfo, "undo", "terminal_authority_nonterminal_failclosed_count",
			 fmt_int64((int64)cluster_terminal_authority_nonterminal_failclosed_count()));
	emit_row(rsinfo, "undo", "terminal_authority_durable_failclosed_count",
			 fmt_int64((int64)cluster_terminal_authority_durable_failclosed_count()));
	emit_row(rsinfo, "undo", "terminal_authority_retention_failclosed_count",
			 fmt_int64((int64)cluster_terminal_authority_retention_failclosed_count()));

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

	/* spec-4.8ab D7: 4 checkpoint-writeback boundary counters (grown into the
	 * existing undo buffer pool region -- D0 finding-3, no new region). */
	emit_row(rsinfo, "undo", "undo_buf_held_wal",
			 fmt_int64((int64)cluster_undo_buf_get_writeback_held_wal_count()));
	emit_row(rsinfo, "undo", "undo_buf_held_evidence",
			 fmt_int64((int64)cluster_undo_buf_get_writeback_held_evidence_count()));
	emit_row(rsinfo, "undo", "undo_buf_boundary_violations",
			 fmt_int64((int64)cluster_undo_buf_get_boundary_violation_count()));
	emit_row(rsinfo, "undo", "undo_buf_remote_evidence_holds",
			 fmt_int64((int64)cluster_undo_buf_get_remote_evidence_hold_count()));
}

/*
 * dump_cr -- spec-3.9 D8 own-instance CR counter observability.
 *
 *	Emits 41 rows under category='cr' (9 spec-3.9 own-instance CR + 4 spec-3.10
 *	L1 cache + 4 spec-3.22 xmax + 5 spec-5.53 identity/reuse-fence mismatch +
 *	8 spec-5.54 tuple-fast-path + 5 spec-5.56 lifecycle + 6 spec-6.12b
 *	CR-server rows) plus the 'cr_pool' (spec-5.51) and admission (spec-5.52)
 *	rows.  Backs cluster_tap t/215 + t/311.
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
	/* spec-6.12b: CR-server data plane (cr category 35 -> 41 rows). */
	emit_row(rsinfo, "cr", "cr_remote_full_count",
			 fmt_int64((int64)cluster_cr_remote_full_count()));
	emit_row(rsinfo, "cr", "cr_remote_partial_count",
			 fmt_int64((int64)cluster_cr_remote_partial_count()));
	emit_row(rsinfo, "cr", "cr_remote_failed_count",
			 fmt_int64((int64)cluster_cr_remote_failed_count()));
	emit_row(rsinfo, "cr", "cr_server_full_count",
			 fmt_int64((int64)cluster_cr_server_full_count()));
	emit_row(rsinfo, "cr", "cr_server_partial_count",
			 fmt_int64((int64)cluster_cr_server_partial_count()));
	emit_row(rsinfo, "cr", "cr_server_denied_count",
			 fmt_int64((int64)cluster_cr_server_denied_count()));
	/* spec-3.22 D3: xmax recycled-slot resolve outcome buckets. */
	emit_row(rsinfo, "cr", "cr_xmax_resolved_count",
			 fmt_int64((int64)cluster_cr_xmax_resolved_count()));
	emit_row(rsinfo, "cr", "cr_xmax_recycled_invisible_count",
			 fmt_int64((int64)cluster_cr_xmax_recycled_invisible_count()));
	emit_row(rsinfo, "cr", "cr_xmax_invalid_or_ambiguous_count",
			 fmt_int64((int64)cluster_cr_xmax_invalid_or_ambiguous_count()));
	emit_row(rsinfo, "cr", "cr_xmax_scan_unavail_or_no_proof_count",
			 fmt_int64((int64)cluster_cr_xmax_scan_unavail_or_no_proof_count()));
	/*
	 * spec-5.53 D5: CR key-identity / reuse-fence mismatch diagnostics (5 rows;
	 * the L1+L2 epoch fence + ABA + over-miss near-miss buckets).  All 0 when the
	 * pool is disabled.  locator_reuse_reject is the catalog-incarnation belt's
	 * 8.A safety-gate observable and stays 0 in this build (D0 = RED → floor-only;
	 * relfilenode-reuse MISSes are attributed to cr_epoch_mismatch_count).
	 */
	emit_row(rsinfo, "cr", "cr_key_mismatch_count",
			 fmt_int64((int64)cluster_cr_pool_key_mismatch_count()));
	emit_row(rsinfo, "cr", "cr_epoch_mismatch_count",
			 fmt_int64((int64)cluster_cr_pool_epoch_mismatch_count()));
	emit_row(rsinfo, "cr", "cr_generation_mismatch_count",
			 fmt_int64((int64)cluster_cr_pool_generation_mismatch_count()));
	emit_row(rsinfo, "cr", "cr_base_lsn_mismatch_count",
			 fmt_int64((int64)cluster_cr_pool_base_lsn_mismatch_count()));
	emit_row(rsinfo, "cr", "cr_locator_reuse_reject_count",
			 fmt_int64((int64)cluster_cr_pool_locator_reuse_reject_count()));
	/* spec-5.54 D5: tuple-level / verdict-only fast-path outcome counters
	 * (independent region; advisory, never feed a verdict).  1 verdict + 7
	 * fallback reasons; all 0 when cluster.cr_tuple_level_fastpath is off. */
	emit_row(rsinfo, "cr", "cr_tuple_verdict_count",
			 fmt_int64((int64)cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_VERDICT)));
	emit_row(rsinfo, "cr", "cr_tuple_fallback_remote_count",
			 fmt_int64((int64)cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_REMOTE)));
	emit_row(rsinfo, "cr", "cr_tuple_fallback_recycle_wm_count",
			 fmt_int64((int64)cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_RECYCLE_WM)));
	emit_row(rsinfo, "cr", "cr_tuple_fallback_multichain_count",
			 fmt_int64((int64)cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_MULTICHAIN)));
	emit_row(rsinfo, "cr", "cr_tuple_fallback_cliff_count",
			 fmt_int64((int64)cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_CLIFF)));
	emit_row(rsinfo, "cr", "cr_tuple_fallback_identity_count",
			 fmt_int64((int64)cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_IDENTITY)));
	emit_row(rsinfo, "cr", "cr_tuple_fallback_cross_block_count",
			 fmt_int64((int64)cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_CROSS_BLOCK)));
	emit_row(rsinfo, "cr", "cr_tuple_fallback_uncertain_count",
			 fmt_int64((int64)cluster_cr_tuple_stat_count(CR_TUPLE_OUTCOME_FALLBACK_UNCERTAIN)));
	/*
	 * spec-5.56 D5: CR pool lifecycle / invalidation counters (5 rows; folded into
	 * the 'cr' category, no new region — 5.53 mismatch-counter precedent).  All 0
	 * when the pool / gen table is disabled.  global_epoch_fallback_bump = coarse
	 * fallback frequency; rel_gen_bump = fine-grained per-relation bumps (Part B
	 * GO, the L373 "hot block survives unrelated DDL" witness); rel_gen_table_
	 * overflow = install serve-but-skip-cache (capacity / NO-GO signal);
	 * retention_horizon_advance_noted = C3 observation (image survived a horizon
	 * advance, still correct); reconfig_intra_survived = C4 unit/injection evidence.
	 */
	emit_row(rsinfo, "cr", "cr_global_epoch_fallback_bump_count",
			 fmt_int64((int64)cluster_cr_pool_global_epoch_fallback_bump_count()));
	emit_row(rsinfo, "cr", "cr_rel_gen_bump_count",
			 fmt_int64((int64)cluster_cr_pool_rel_gen_bump_count()));
	emit_row(rsinfo, "cr", "cr_rel_gen_table_overflow_count",
			 fmt_int64((int64)cluster_cr_pool_rel_gen_table_overflow_count()));
	emit_row(rsinfo, "cr", "cr_retention_horizon_advance_noted_count",
			 fmt_int64((int64)cluster_cr_pool_retention_horizon_advance_noted_count()));
	emit_row(rsinfo, "cr", "cr_reconfig_intra_survived_count",
			 fmt_int64((int64)cluster_cr_pool_reconfig_intra_survived_count()));
	/* spec-5.51 D9: dedicated shared CR buffer pool (L2) counters.  All 0 when
	 * the pool is disabled (cluster.shared_cr_pool_size_blocks == 0). */
	emit_row(rsinfo, "cr_pool", "current_epoch", fmt_int64((int64)cluster_cr_pool_current_epoch()));
	emit_row(rsinfo, "cr_pool", "live_entries", fmt_int64((int64)cluster_cr_pool_live_entries()));
	emit_row(rsinfo, "cr_pool", "hit_count", fmt_int64((int64)cluster_cr_pool_hit_count()));
	emit_row(rsinfo, "cr_pool", "miss_count", fmt_int64((int64)cluster_cr_pool_miss_count()));
	emit_row(rsinfo, "cr_pool", "reserve_count", fmt_int64((int64)cluster_cr_pool_reserve_count()));
	emit_row(rsinfo, "cr_pool", "publish_count", fmt_int64((int64)cluster_cr_pool_publish_count()));
	emit_row(rsinfo, "cr_pool", "abort_count", fmt_int64((int64)cluster_cr_pool_abort_count()));
	emit_row(rsinfo, "cr_pool", "evict_count", fmt_int64((int64)cluster_cr_pool_evict_count()));
	emit_row(rsinfo, "cr_pool", "epoch_bump_count",
			 fmt_int64((int64)cluster_cr_pool_epoch_bump_count()));
	emit_row(rsinfo, "cr_pool", "publish_stale_release_count",
			 fmt_int64((int64)cluster_cr_pool_publish_stale_release_count()));

	/* spec-5.52 D9: admission reason counters (independent region; advisory). */
	emit_row(rsinfo, "cr_pool", "admit_count",
			 fmt_int64((int64)cluster_cr_admit_stat_count(CR_ADMIT_REASON_ADMITTED)));
	emit_row(rsinfo, "cr_pool", "admit_reject_no_admit",
			 fmt_int64((int64)cluster_cr_admit_stat_count(CR_ADMIT_REASON_NO_ADMIT)));
	emit_row(rsinfo, "cr_pool", "admit_reject_bulk",
			 fmt_int64((int64)cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_BULK)));
	emit_row(rsinfo, "cr_pool", "admit_reject_parallel",
			 fmt_int64((int64)cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_PARALLEL)));
	emit_row(rsinfo, "cr_pool", "admit_reject_nonmain_fork",
			 fmt_int64((int64)cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_NONMAIN_FORK)));
	emit_row(rsinfo, "cr_pool", "admit_reject_volatile",
			 fmt_int64((int64)cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_VOLATILE)));
	emit_row(rsinfo, "cr_pool", "admit_reject_relcap",
			 fmt_int64((int64)cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_RELCAP)));
	emit_row(rsinfo, "cr_pool", "admit_reject_pressure",
			 fmt_int64((int64)cluster_cr_admit_stat_count(CR_ADMIT_REASON_REJECT_PRESSURE)));

	/* spec-5.55 D8: shared resolver cache (CR Source 3 by-xid search-shortcut)
	 * counters.  All 0 unless resolver_cache_enabled / _measure is on.  These feed
	 * the §0.6 value gate: redundancy = key_present / lookup, re-probe hit rate =
	 * hit / key_present, acceptance pass rate = acceptance_pass / hit. */
	emit_row(rsinfo, "resolver_cache", "lookup",
			 fmt_int64((int64)cluster_resolver_cache_lookup_count()));
	emit_row(rsinfo, "resolver_cache", "key_present",
			 fmt_int64((int64)cluster_resolver_cache_key_present_count()));
	emit_row(rsinfo, "resolver_cache", "epoch_miss",
			 fmt_int64((int64)cluster_resolver_cache_epoch_miss_count()));
	emit_row(rsinfo, "resolver_cache", "hit", fmt_int64((int64)cluster_resolver_cache_hit_count()));
	emit_row(rsinfo, "resolver_cache", "revalidate_miss",
			 fmt_int64((int64)cluster_resolver_cache_revalidate_miss_count()));
	emit_row(rsinfo, "resolver_cache", "acceptance_pass",
			 fmt_int64((int64)cluster_resolver_cache_acceptance_pass_count()));
	emit_row(rsinfo, "resolver_cache", "acceptance_failclosed",
			 fmt_int64((int64)cluster_resolver_cache_acceptance_failclosed_count()));
	emit_row(rsinfo, "resolver_cache", "install",
			 fmt_int64((int64)cluster_resolver_cache_install_count()));
	emit_row(rsinfo, "resolver_cache", "evict",
			 fmt_int64((int64)cluster_resolver_cache_evict_count()));
	emit_row(rsinfo, "resolver_cache", "nonown_skip",
			 fmt_int64((int64)cluster_resolver_cache_nonown_skip_count()));
	emit_row(rsinfo, "resolver_cache", "nonterminal_skip",
			 fmt_int64((int64)cluster_resolver_cache_nonterminal_skip_count()));
	emit_row(rsinfo, "resolver_cache", "live_entries",
			 fmt_int64((int64)cluster_resolver_cache_live_entries()));

	/* spec-5.57 D3: cross-instance CR read-path coordinator boundary counters
	 * (independent region; advisory; category 'cr_coord').  Always emitted so the
	 * category is deterministically present.  Values stay 0 under
	 * cluster.cross_instance_cr_coordinator=off (the fail-closed 53R9G boundary
	 * still fires; only the bumps are gated, §2.2). */
	emit_row(
		rsinfo, "cr_coord", cluster_cr_coordinator_counter_key(CR_COORD_CROSS_INSTANCE_CR_REFUSED),
		fmt_int64((int64)cluster_cr_coordinator_stat_count(CR_COORD_CROSS_INSTANCE_CR_REFUSED)));
	emit_row(
		rsinfo, "cr_coord", cluster_cr_coordinator_counter_key(CR_COORD_REMOTE_UNDO_READ_REFUSED),
		fmt_int64((int64)cluster_cr_coordinator_stat_count(CR_COORD_REMOTE_UNDO_READ_REFUSED)));
	emit_row(
		rsinfo, "cr_coord", cluster_cr_coordinator_counter_key(CR_COORD_MATERIALIZED_REMOTE_SERVED),
		fmt_int64((int64)cluster_cr_coordinator_stat_count(CR_COORD_MATERIALIZED_REMOTE_SERVED)));
	emit_row(rsinfo, "cr_coord",
			 cluster_cr_coordinator_counter_key(CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE),
			 fmt_int64(
				 (int64)cluster_cr_coordinator_stat_count(CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE)));
}


/*
 * dump_wal_thread -- spec-4.1 D7 per-thread WAL routing state.
 *
 *	Emits 5 rows under category='wal_thread'.  thread_id /
 *	dir_configured / dir_validated / claim_created are startup-time
 *	snapshots mirrored in the 'pgrac wal thread' shmem region (written
 *	once by cluster_wal_thread_init, EXEC_BACKEND-safe);
 *	page_stamp_count is the accumulator bumped per real-id WAL page
 *	stamp (zero whenever the node stamps LEGACY -- L29 silence matrix).
 */
static void
dump_wal_thread(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "wal_thread", "thread_id",
			 fmt_int64((int64)cluster_wal_thread_dump_thread_id()));
	emit_row(rsinfo, "wal_thread", "dir_configured", fmt_bool(cluster_wal_thread_dir_configured()));
	emit_row(rsinfo, "wal_thread", "dir_validated", fmt_bool(cluster_wal_thread_dir_validated()));
	emit_row(rsinfo, "wal_thread", "claim_created", fmt_bool(cluster_wal_thread_claim_created()));
	emit_row(rsinfo, "wal_thread", "page_stamp_count",
			 fmt_int64((int64)cluster_wal_thread_page_stamp_count()));

	/* spec-4.2 D5: ClusterWalState registry keys (5 -> 10; live reads). */
	{
		ClusterWalStateSlot slot;
		bool ready = cluster_wal_state_registry_ready();
		ClusterWalSlotVerdict v = CLUSTER_WAL_SLOT_EMPTY;

		if (ready)
			v = cluster_wal_state_read_slot(cluster_wal_thread_dump_thread_id(), &slot);

		emit_row(rsinfo, "wal_thread", "registry_ready", fmt_bool(ready));
		emit_row(rsinfo, "wal_thread", "registry_slot_state",
				 !ready
					 ? "-"
					 : (v == CLUSTER_WAL_SLOT_OK
							? (slot.state == CLUSTER_WAL_SLOT_STATE_ACTIVE ? "active" : "stopped")
							: (v == CLUSTER_WAL_SLOT_EMPTY ? "empty" : "unknown")));
		emit_row(rsinfo, "wal_thread", "registry_last_updated",
				 (ready && v == CLUSTER_WAL_SLOT_OK) ? fmt_int64(slot.last_updated) : "-");
		emit_row(rsinfo, "wal_thread", "registry_highest_lsn",
				 (ready && v == CLUSTER_WAL_SLOT_OK) ? fmt_uint64_hex(slot.highest_lsn) : "-");
		emit_row(rsinfo, "wal_thread", "registry_highest_scn",
				 (ready && v == CLUSTER_WAL_SLOT_OK) ? fmt_int64((int64)slot.highest_scn) : "-");
	}
}

/*
 * dump_write_fence -- spec-4.12 D7 + spec-4.12b D6.  Emits 8 rows under
 *	category='write_fence': the 4 spec-4.12 cooperative write-fence counters plus the
 *	4 spec-4.12b baseline-subsystem observability fields (L110-safe -- read 0 with no
 *	region attached).
 */
static void
dump_write_fence(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "write_fence", "hot_gate_blocked",
			 fmt_int64((int64)cluster_write_fence_get_hot_gate_blocked()));
	emit_row(rsinfo, "write_fence", "durable_check_blocked",
			 fmt_int64((int64)cluster_write_fence_get_durable_check_blocked()));
	emit_row(rsinfo, "write_fence", "minority_marker_ignored",
			 fmt_int64((int64)cluster_write_fence_get_minority_marker_ignored()));
	emit_row(rsinfo, "write_fence", "marker_write_failed",
			 fmt_int64((int64)cluster_write_fence_get_marker_write_failed()));
	/* spec-4.12b D6: baseline subsystem observability. */
	emit_row(rsinfo, "write_fence", "baseline_published",
			 fmt_int64((int64)cluster_write_fence_get_baseline_published()));
	emit_row(rsinfo, "write_fence", "baseline_stale_rejected",
			 fmt_int64((int64)cluster_write_fence_get_baseline_stale_rejected()));
	emit_row(rsinfo, "write_fence", "baseline_author_is_self",
			 fmt_int64((int64)(cluster_write_fence_get_baseline_author_is_self() ? 1 : 0)));
	emit_row(rsinfo, "write_fence", "baseline_authority_age_us",
			 fmt_int64((int64)cluster_write_fence_get_baseline_authority_age_us()));
}

/*
 * dump_hw -- spec-5.7 HW (relation-extend) authority observability.  The
 * counters surface the first-sight establishment (§3.1c) and the online-remaster
 * HW rebuild (§3.1b R4/R9): remaster_done / remaster_blocked make the survivor's
 * HW authority rebuild after a master death directly observable (S7), and
 * not_ready / failclosed surface the 53RA6 fail-closed serve gate.
 */
static void
dump_hw(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "hw", "alloc_count", fmt_int64((int64)cluster_hw_alloc_count()));
	emit_row(rsinfo, "hw", "authority_create_count",
			 fmt_int64((int64)cluster_hw_authority_create_count()));
	emit_row(rsinfo, "hw", "reserve_wal_count", fmt_int64((int64)cluster_hw_reserve_wal_count()));
	emit_row(rsinfo, "hw", "rebuild_count", fmt_int64((int64)cluster_hw_rebuild_count()));
	emit_row(rsinfo, "hw", "failclosed_count", fmt_int64((int64)cluster_hw_failclosed_count()));
	emit_row(rsinfo, "hw", "not_ready_count", fmt_int64((int64)cluster_hw_not_ready_count()));
	emit_row(rsinfo, "hw", "remaster_done_count",
			 fmt_int64((int64)cluster_hw_remaster_done_count()));
	emit_row(rsinfo, "hw", "remaster_blocked_count",
			 fmt_int64((int64)cluster_hw_remaster_blocked_count()));

	/*
	 * spec-6.12d D-obs: space-lease counters.  bloat_ratio itself is a
	 * per-relation D0-gate metric the harness computes from these plus
	 * relation size; outstanding = leased_total - consumed - orphan_zero
	 * is the live zero-page inventory across all active leases.
	 */
	if (ClusterHwLeaseCtl != NULL) {
		uint64 leased = pg_atomic_read_u64(&ClusterHwLeaseCtl->d_leased_total);
		uint64 consumed = pg_atomic_read_u64(&ClusterHwLeaseCtl->d_consumed);
		uint64 orphan = pg_atomic_read_u64(&ClusterHwLeaseCtl->d_orphan_zero);

		emit_row(rsinfo, "hw", "lease_leased_total", fmt_int64((int64)leased));
		emit_row(rsinfo, "hw", "lease_consumed", fmt_int64((int64)consumed));
		emit_row(rsinfo, "hw", "lease_orphan_zero", fmt_int64((int64)orphan));
		emit_row(rsinfo, "hw", "lease_grants",
				 fmt_int64((int64)pg_atomic_read_u64(&ClusterHwLeaseCtl->d_lease_grants)));
		emit_row(rsinfo, "hw", "lease_outstanding", fmt_int64((int64)(leased - consumed - orphan)));
	}
}

/*
 * dump_dl -- spec-5.7 §3.2 DL (bulk-load lease) observability.  lease_count is
 * the faithful proof that a bulk load took the real cross-node DL(X) GES lease;
 * native_count counts uncoordinated proceeds; failclosed_count counts 53RA7;
 * release_count counts coordinated leases released (incl. the xact-end backstop
 * for an aborted bulk load -- lease_count > release_count for long = a leak).
 */
static void
dump_dl(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "dl", "lease_count", fmt_int64((int64)cluster_dl_lease_count()));
	emit_row(rsinfo, "dl", "native_count", fmt_int64((int64)cluster_dl_native_count()));
	emit_row(rsinfo, "dl", "failclosed_count", fmt_int64((int64)cluster_dl_failclosed_count()));
	emit_row(rsinfo, "dl", "release_count", fmt_int64((int64)cluster_dl_release_count()));
}

/*
 * dump_ir -- spec-5.7 §3.4 IR (instance-recovery owner) observability.
 * owner_count is the faithful proof that a survivor took the real GES-enforced
 * IR(X) recovery-owner lock before its destructive thread-recovery apply;
 * conflict_count counts the 53RA9 non-owner fail-closed path (a survivor whose
 * alive-set view diverged and lost the IR(X) claim); native_count counts the
 * single-node / no-competitor proceeds; release_count counts owner claims
 * released after the apply.
 */
static void
dump_ir(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "ir", "owner_count", fmt_int64((int64)cluster_ir_owner_count()));
	emit_row(rsinfo, "ir", "native_count", fmt_int64((int64)cluster_ir_native_count()));
	emit_row(rsinfo, "ir", "conflict_count", fmt_int64((int64)cluster_ir_conflict_count()));
	emit_row(rsinfo, "ir", "release_count", fmt_int64((int64)cluster_ir_release_count()));
}

/*
 * dump_ts -- spec-5.7 §3.3 TT (tablespace-DDL) lock observability.  x_count is
 * the cross-node DDL mutex (CREATE/DROP/ALTER/RENAME TABLESPACE) grants; s_count
 * the placement-DDL in-use guard grants; native_count the single-node proceeds;
 * failclosed_count the 53RA8 conflict fail-closed (a peer held a conflicting
 * TT(X)/TT(S)).
 */
static void
dump_ts(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "ts", "x_count", fmt_int64((int64)cluster_ts_x_count()));
	emit_row(rsinfo, "ts", "s_count", fmt_int64((int64)cluster_ts_s_count()));
	emit_row(rsinfo, "ts", "native_count", fmt_int64((int64)cluster_ts_native_count()));
	emit_row(rsinfo, "ts", "failclosed_count", fmt_int64((int64)cluster_ts_failclosed_count()));
}

/*
 * dump_ko -- spec-5.7 §3.5/§3.6 KO (object-reuse flush) barrier observability.
 * flush_count is barriers initiated by a dropping node; ack_received_count the
 * per-peer apply-after-drop DONE ACKs recorded; failclosed_count the 53RAA
 * fail-closed (a peer did not flush in time / KO(X) unavailable); native_count
 * the no-op cases (single-node / no alive peer / private relation);
 * lockfail_count the best-effort KO(X) acquires that failed (the barrier still
 * ran -- KO(X) is auxiliary serialisation, not the correctness gate);
 * peer_apply_count the local flush+drop applied + ACK'd as a peer;
 * inbound_full_count the KO inbound ring overflows (peer dropped a request).
 */
static void
dump_ko(ReturnSetInfo *rsinfo)
{
	emit_row(rsinfo, "ko", "flush_count", fmt_int64((int64)cluster_ko_flush_count()));
	emit_row(rsinfo, "ko", "ack_received_count", fmt_int64((int64)cluster_ko_ack_received_count()));
	emit_row(rsinfo, "ko", "failclosed_count", fmt_int64((int64)cluster_ko_failclosed_count()));
	emit_row(rsinfo, "ko", "native_count", fmt_int64((int64)cluster_ko_native_count()));
	emit_row(rsinfo, "ko", "lockfail_count", fmt_int64((int64)cluster_ko_lockfail_count()));
	emit_row(rsinfo, "ko", "peer_apply_count", fmt_int64((int64)cluster_ko_peer_apply_count()));
	emit_row(rsinfo, "ko", "inbound_full_count", fmt_int64((int64)cluster_ko_inbound_full_count()));
}

/*
 * dump_xnode_profile -- spec-5.59 cross-node profiling buckets.
 *
 *	Emits bucket.<name>.total_nanos / bucket.<name>.n_events for every
 *	ClusterXnodeBucket plus the probe keys (reset_generation, read-side
 *	amortization probe, HW extend locality split).  Keys are emitted even
 *	while cluster.xnode_profile is off (values stay 0) so the key surface
 *	is stable for tests and samplers.
 */
static void
dump_xnode_profile(ReturnSetInfo *rsinfo)
{
	ClusterXnodeProfileShared *ctl = ClusterXnodeProfileCtl;

	for (int i = 0; i < CLXP_NBUCKETS; i++) {
		const char *name = cluster_xp_bucket_name((ClusterXnodeBucket)i);
		char key[96];
		uint64 nanos = 0;
		uint64 events = 0;

		if (ctl != NULL) {
			nanos = pg_atomic_read_u64(&ctl->bucket[i].total_nanos);
			events = pg_atomic_read_u64(&ctl->bucket[i].n_events);
		}
		snprintf(key, sizeof(key), "bucket.%s.total_nanos", name);
		emit_row(rsinfo, "xnode_profile", key, fmt_int64((int64)nanos));
		snprintf(key, sizeof(key), "bucket.%s.n_events", name);
		emit_row(rsinfo, "xnode_profile", key, fmt_int64((int64)events));
	}

	emit_row(rsinfo, "xnode_profile", "reset_generation",
			 fmt_int64(ctl != NULL ? (int64)pg_atomic_read_u64(&ctl->reset_generation) : 0));
	emit_row(rsinfo, "xnode_profile", "read_reship_count",
			 fmt_int64(ctl != NULL ? (int64)pg_atomic_read_u64(&ctl->read_reship_count) : 0));
	emit_row(rsinfo, "xnode_profile", "read_sholder_hit_count",
			 fmt_int64(ctl != NULL ? (int64)pg_atomic_read_u64(&ctl->read_sholder_hit_count) : 0));
	emit_row(rsinfo, "xnode_profile", "hw_extend_local_count",
			 fmt_int64(ctl != NULL ? (int64)pg_atomic_read_u64(&ctl->hw_extend_local_count) : 0));
	emit_row(rsinfo, "xnode_profile", "hw_extend_remote_count",
			 fmt_int64(ctl != NULL ? (int64)pg_atomic_read_u64(&ctl->hw_extend_remote_count) : 0));
}

/*
 * dump_xnode_lever -- spec-6.12 per-wave lever counters.
 *
 *	Wave-prefixed keys (c_* = wave 6.12c resolver memo + D0 stamp-evidence
 *	classification).  Keys are emitted even while every wave GUC is off
 *	(values stay 0) so the key surface is stable for tests and samplers;
 *	later waves append keys, never rename (5.59 Q9-B category paradigm).
 */
static void
dump_xnode_lever(ReturnSetInfo *rsinfo)
{
	ClusterXnodeLeverShared *ctl = ClusterXnodeLeverCtl;

#define XNL_ROW(field)                                                                             \
	emit_row(rsinfo, "xnode_lever", #field,                                                        \
			 fmt_int64(ctl != NULL ? (int64)pg_atomic_read_u64(&ctl->field) : 0))
	XNL_ROW(c_resolve_count);
	XNL_ROW(c_tt_lookup_count);
	XNL_ROW(c_memo_hit_count);
	XNL_ROW(c_memo_install_count);
	XNL_ROW(c_stamp_cached_seen_count);
	XNL_ROW(c_stamp_contradicted_count);
	XNL_ROW(a_downgrade_count);
	XNL_ROW(a_downgrade_refused_count);
	XNL_ROW(a_fwd_oneshot_count);
	XNL_ROW(a_remote_downgrade_count);
	XNL_ROW(a_remote_downgrade_refused_count);
	XNL_ROW(a_remote_ack_degraded_count);
	XNL_ROW(e1_drain_count);
	XNL_ROW(e1_grant_count);
	XNL_ROW(e1_invariant_violation_count);
	XNL_ROW(g_active_itl_transfer_count);
	XNL_ROW(g_stamp_skipped_count);
	XNL_ROW(g_drift_resolved_via_tt_count);
#undef XNL_ROW
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
		dump_cf(rsinfo);
		dump_smart_fusion(rsinfo);
		dump_phase(rsinfo);
		dump_lmon(rsinfo);
		dump_lck(rsinfo);
		dump_diag(rsinfo);
		dump_hang(rsinfo);
		dump_cluster_stats(rsinfo);
		dump_cluster_cssd(rsinfo);
		dump_undo_cleaner(rsinfo);
		dump_visibility(rsinfo);
		dump_tt_2pc(rsinfo);
		dump_recovery(rsinfo);
		dump_reconfig_touched(rsinfo); /* spec-5.14 D6 */
		dump_reconfig_join(rsinfo);	   /* spec-5.15 D6 */
		dump_scn(rsinfo);
		dump_ges(rsinfo);
		dump_advisory(rsinfo);
		dump_grd(rsinfo);
		dump_grd_recovery(rsinfo);
		dump_lmd(rsinfo);
		dump_lms(rsinfo);
		dump_undo(rsinfo);
		dump_cr(rsinfo);
		dump_wal_thread(rsinfo);
		dump_write_fence(rsinfo);
		dump_hw(rsinfo);
		dump_dl(rsinfo);
		dump_ir(rsinfo);
		dump_ts(rsinfo);
		dump_ko(rsinfo);
		dump_xnode_profile(rsinfo); /* spec-5.59 D1 */
		dump_xnode_lever(rsinfo);	/* spec-6.12 */
	}
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_state requires --enable-cluster")));
#endif

	return (Datum)0;
}


#ifndef USE_PGRAC_CLUSTER
/*
 * spec-5.9 fix-forward — pg_cluster_hang_dump (spec-5.11) is declared in
 * pg_proc.dat, but its definition lives in cluster_hang.c, which is NOT part of
 * the --disable-cluster minimal OBJS, leaving fmgrtab with an undefined
 * reference and breaking the --disable-cluster build.  cluster_debug.o IS in
 * the minimal OBJS, so provide the standard disabled stub here.  Under
 * --enable-cluster this block is skipped and cluster_hang.c owns the symbol.
 */
PG_FUNCTION_INFO_V1(pg_cluster_hang_dump);
Datum
pg_cluster_hang_dump(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_hang_dump requires --enable-cluster")));
	return (Datum)0;
}
#endif /* !USE_PGRAC_CLUSTER */
