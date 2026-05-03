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
#include "cluster/cluster_ic.h"			   /* ClusterICOps_Active, ClusterICTier */
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
	}
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_state requires --enable-cluster")));
#endif

	return (Datum)0;
}
