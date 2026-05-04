/*-------------------------------------------------------------------------
 *
 * cluster_views.c
 *	  pgrac cluster system view backing functions (SRFs).
 *
 *	  Stage 0.16 introduces the cluster pg_stat_cluster_* view framework.
 *	  This file is the single C entry point for all cluster view SRFs:
 *	  each future subsystem spec adds its own SRF function here, plus a
 *	  matching pg_proc.dat entry and a system_views.sql VIEW declaration.
 *
 *	  Stage 0.16 ships ONE SRF: cluster_get_wait_events, backing the
 *	  pg_stat_cluster_wait_events view.  It iterates the 51 cluster wait
 *	  event values registered by spec-0.11 and emits one row per event
 *	  with (type, name) populated by the existing pgstat_get_wait_event
 *	  / pgstat_get_wait_event_type lookups.
 *
 *	  Why ONE SRF and not the full ~40 in performance-views-design.md
 *	  §2.2: the other views require runtime data sources that do not
 *	  exist at Stage 0 (no LMS / GRD / Heartbeat / Recovery code yet).
 *	  Registering a placeholder SRF that returns 0 rows would mislead
 *	  DBAs (they would assume the subsystem exists but is idle).  See
 *	  CLAUDE.md rule 8 and the registration policy in
 *	  docs/cluster-views-impl-design.md §0.2.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_views.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The SRF tuplestore pattern below mirrors PG's own examples (e.g.
 *	  pg_get_replication_slots, pg_stat_get_subscription_stats).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT (stage 0.30 sweep) */
#include "cluster/cluster_views.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_conf.h"			/* cluster_conf_lookup_node, role helpers */
#include "cluster/cluster_guc.h"			/* cluster_node_id */
#include "cluster/cluster_shmem.h"			/* ClusterShmem->created_at */
#include "cluster/cluster_version_macros.h" /* PGRAC_VERSION_STRING */
#endif


/* ============================================================
 * cluster_get_wait_events -- backing SRF for pg_stat_cluster_wait_events
 *
 *	The function symbol is provided unconditionally so that pg_proc.dat
 *	can reference it from both --enable-cluster and --disable-cluster
 *	builds (initdb loads the catalog identically in both modes).  In
 *	--disable-cluster builds the function returns an empty result set
 *	since no cluster wait events are registered or routed through the
 *	pgstat dispatch table.
 * ============================================================ */

PG_FUNCTION_INFO_V1(cluster_get_wait_events);

#ifdef USE_PGRAC_CLUSTER

/*
 * Static table of cluster wait events known to the registration table.
 *
 *	This list mirrors the WaitEventCluster enum in
 *	src/include/utils/wait_event.h (registered by spec-0.11) and the
 *	per-class strings emitted by pgstat_get_wait_event_type() in
 *	src/backend/utils/activity/wait_event.c.
 *
 *	Order matters only insofar as it determines the order rows are
 *	emitted; the SQL view does not promise an ordering.
 */
static const uint32 cluster_wait_event_infos[CLUSTER_WAIT_EVENTS_COUNT] = {
	/* Cluster: GES (5) */
	WAIT_EVENT_GES_ENQUEUE_ACQUIRE,
	WAIT_EVENT_GES_ENQUEUE_CONVERT,
	WAIT_EVENT_GES_ENQUEUE_RELEASE_ACK,
	WAIT_EVENT_GES_MASTER_QUERY,
	WAIT_EVENT_GES_LOCAL_FAST_PATH,

	/* Cluster: PCM (6) */
	WAIT_EVENT_PCM_BLOCK_READ_N_S,
	WAIT_EVENT_PCM_BLOCK_READ_N_X,
	WAIT_EVENT_PCM_BLOCK_WRITE_S_X,
	WAIT_EVENT_PCM_BLOCK_CONVERT_WAIT,
	WAIT_EVENT_PCM_BLOCK_DOWNGRADE,
	WAIT_EVENT_PCM_ITL_CLEANOUT,

	/* Cluster: BufferShip (5) */
	WAIT_EVENT_BUFFER_SHIP_CR_BUILD,
	WAIT_EVENT_BUFFER_SHIP_CR_SEND,
	WAIT_EVENT_BUFFER_SHIP_CR_RECEIVE,
	WAIT_EVENT_BUFFER_SHIP_CURRENT_SEND,
	WAIT_EVENT_BUFFER_SHIP_CURRENT_RECEIVE,

	/* Cluster: SCN (4) */
	WAIT_EVENT_SCN_BOC_FLUSH_WAIT,
	WAIT_EVENT_SCN_PIGGYBACK_MERGE,
	WAIT_EVENT_SCN_CROSS_NODE_COMPARE,
	WAIT_EVENT_SCN_ADVANCE_BROADCAST,

	/* Cluster: Reconfig (5) */
	WAIT_EVENT_RECONFIG_GRD_REBUILD,
	WAIT_EVENT_RECONFIG_LOCK_RECOVERY,
	WAIT_EVENT_RECONFIG_FENCE_WAIT,
	WAIT_EVENT_RECONFIG_MASTER_SELECTION,
	WAIT_EVENT_RECONFIG_BARRIER_WAIT,

	/* Cluster: Recovery (5) */
	WAIT_EVENT_RECOVERY_WAL_FETCH,
	WAIT_EVENT_RECOVERY_KWAY_MERGE,
	WAIT_EVENT_RECOVERY_APPLY_PER_THREAD,
	WAIT_EVENT_RECOVERY_UNDO_REPLAY,
	WAIT_EVENT_RECOVERY_PCM_STATE_RESTORE,

	/* Cluster: Sinval (3) */
	WAIT_EVENT_SINVAL_BROADCAST_SEND,
	WAIT_EVENT_SINVAL_BROADCAST_RECEIVE,
	WAIT_EVENT_SINVAL_INJECT_LOCAL_QUEUE,

	/* Cluster: Interconnect (5) */
	WAIT_EVENT_INTERCONNECT_RDMA_SEND,
	WAIT_EVENT_INTERCONNECT_RDMA_RECV,
	WAIT_EVENT_INTERCONNECT_TCP_FALLBACK,
	WAIT_EVENT_INTERCONNECT_TIER_SWITCH,
	WAIT_EVENT_INTERCONNECT_CONNECT_RETRY,

	/* Cluster: Undo (4) */
	WAIT_EVENT_UNDO_REMOTE_READ,
	WAIT_EVENT_UNDO_TT_LOOKUP_REMOTE,
	WAIT_EVENT_UNDO_SEGMENT_FETCH,
	WAIT_EVENT_UNDO_RETENTION_WAIT,

	/* Cluster: ADG (4) */
	WAIT_EVENT_ADG_MRP_APPLY_WAIT,
	WAIT_EVENT_ADG_WAL_RECEIVE_LAG,
	WAIT_EVENT_ADG_READ_SNAPSHOT_WAIT,
	WAIT_EVENT_ADG_SCN_SYNC_WAIT,

	/* Cluster: SharedFs (5) -- spec-1.1 */
	WAIT_EVENT_CLUSTER_SHARED_FS_READ,
	WAIT_EVENT_CLUSTER_SHARED_FS_WRITE,
	WAIT_EVENT_CLUSTER_SHARED_FS_EXTEND,
	WAIT_EVENT_CLUSTER_SHARED_FS_TRUNCATE,
	WAIT_EVENT_CLUSTER_SHARED_FS_FSYNC,

	/* Cluster: StartupPhase (5) -- spec-1.10 (2026-05-03) */
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_0,
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_1,
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_2,
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_3,
	WAIT_EVENT_CLUSTER_STARTUP_PHASE_4,

	/*
	 * Cluster: BgProc (1) -- spec-1.11 Sprint B (2026-05-04) wait
	 * class for cluster background-process main-loop / lifecycle /
	 * liveness waits.  Spec-1.11.1 F12: missed in original Sprint B
	 * D11 ship; SRF row added retrospectively so LMON main loop is
	 * SQL-visible alongside the C-level enum + name lookup.
	 */
	WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP,
};

/* Compile-time assertion: array length must match the documented count. */
StaticAssertDecl(lengthof(cluster_wait_event_infos) == CLUSTER_WAIT_EVENTS_COUNT,
				 "cluster_wait_event_infos length must equal CLUSTER_WAIT_EVENTS_COUNT");

#endif /* USE_PGRAC_CLUSTER */


Datum
cluster_get_wait_events(PG_FUNCTION_ARGS)
{
	CLUSTER_INJECTION_POINT("cluster-views-srf-entry");

	/*
	 * Use PG's helper to set up a tuplestore-returning SRF.  This pattern
	 * matches pg_stat_get_subscription_stats and similar PG core SRFs.
	 */
	InitMaterializedSRF(fcinfo, 0);

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

		for (int i = 0; i < CLUSTER_WAIT_EVENTS_COUNT; i++) {
			uint32 info = cluster_wait_event_infos[i];
			Datum values[2];
			bool nulls[2] = { false, false };
			const char *type;
			const char *name;

			type = pgstat_get_wait_event_type(info);
			name = pgstat_get_wait_event(info);

			values[0] = CStringGetTextDatum(type ? type : "(unknown)");
			values[1] = CStringGetTextDatum(name ? name : "(unknown)");

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}
#endif

	/*
	 * In --disable-cluster builds we fall through with no rows emitted.
	 * Querying pg_stat_cluster_wait_events then yields an empty result
	 * set, which is the documented behavior for that build mode.
	 */
	return (Datum)0;
}


/* ============================================================
 * cluster_get_gcluster_wait_events -- backing SRF for
 *	pg_stat_gcluster_wait_events.
 *
 *	Stage 0.17 placeholder: emits one row per registered cluster wait
 *	event for the local node only (node_id taken from the cluster.node_id
 *	GUC).  The outer loop over "known nodes" is structured so that future
 *	cross-node RPC fan-out (Stage 6+ AD-007) replaces only the loop body
 *	-- the (node_id, type, name) column contract stays stable from 0.17
 *	onward.  See specs/spec-0.17-gviews-skeleton.md §2.5.
 *
 *	In --disable-cluster builds the SRF returns an empty result set, the
 *	same convention as cluster_get_wait_events.
 * ============================================================ */

PG_FUNCTION_INFO_V1(cluster_get_gcluster_wait_events);

Datum
cluster_get_gcluster_wait_events(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

		/*
		 * Stage 0.17 placeholder for the "known nodes" set.  Stage 6+ will
		 * replace this with a Heartbeat-driven membership query (and the
		 * inner emit branch with an RPC fan-out for non-self nodes).
		 */
		const int32 known_nodes[1] = { (int32)cluster_node_id };
		const int n_nodes = 1;

		for (int n = 0; n < n_nodes; n++) {
			int32 nid = known_nodes[n];

			for (int i = 0; i < CLUSTER_WAIT_EVENTS_COUNT; i++) {
				uint32 info = cluster_wait_event_infos[i];
				Datum values[3];
				bool nulls[3] = { false, false, false };
				const char *type;
				const char *name;

				type = pgstat_get_wait_event_type(info);
				name = pgstat_get_wait_event(info);

				values[0] = Int32GetDatum(nid);
				values[1] = CStringGetTextDatum(type ? type : "(unknown)");
				values[2] = CStringGetTextDatum(name ? name : "(unknown)");

				tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
			}
		}
	}
#endif

	return (Datum)0;
}


/* ============================================================
 * cluster_get_stat_nodes -- backing SRF for pg_stat_cluster_nodes
 * (stage 0.28).
 *
 *	Returns runtime metadata for each cluster node.  Stage 0 always
 *	emits exactly one row (the local node).  Stage 2+ extends to all
 *	nodes by iterating cluster_conf topology; the column shape is
 *	stable from this stage onward.
 *
 *	role     joined from pgrac.conf via cluster_conf_lookup_node;
 *	         "unknown" if the configured cluster_node_id has no
 *	         matching pgrac.conf entry (e.g. fallback single-node).
 *	state    hardcoded "online" -- the only legitimate state for a
 *	         backend that is actively serving SQL.  Stage 4+ recovery
 *	         expands to {online, recovering, down, starting}.
 *
 *	See specs/spec-0.28-perfmon-framework.md §2.5 / §3.3.
 * ============================================================ */

PG_FUNCTION_INFO_V1(cluster_get_stat_nodes);

Datum
cluster_get_stat_nodes(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
		Datum values[6];
		bool nulls[6] = { false, false, false, false, false, false };
		const ClusterNodeInfo *node_info;
		const char *role_str;

		node_info = cluster_conf_lookup_node((int32)cluster_node_id);
		if (node_info != NULL)
			role_str = cluster_conf_role_to_string(node_info->role);
		else
			role_str = "unknown";

		values[0] = Int32GetDatum((int32)cluster_node_id);
		values[1] = CStringGetTextDatum(role_str ? role_str : "unknown");
		values[2] = CStringGetTextDatum("online");

		/*
		 * ClusterShmem->created_at is set in cluster_init_shmem when
		 * the postmaster first allocated the control block (see
		 * cluster_shmem.c::cluster_ctl_shmem_init).  Backend-local
		 * pointer dereference; no locking required (immutable after
		 * postmaster init, before any backend forks).
		 */
		if (ClusterShmem != NULL)
			values[3] = TimestampTzGetDatum(ClusterShmem->created_at);
		else
			nulls[3] = true;

		values[4] = CStringGetTextDatum(PGRAC_VERSION_STRING);
		values[5] = CStringGetTextDatum(PG_VERSION);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
#endif

	return (Datum)0;
}


/* ============================================================
 * cluster_shmem_dump_regions -- backing SRF for pg_cluster_shmem
 * (stage 1.3).
 *
 *	Returns one row per registered cluster shmem region with
 *	(name, size_bytes, lwlock_count, owner_subsys).  Stage 1.3
 *	baseline registers two regions (cluster_ctl + cluster_conf);
 *	Stage 2+ subsystems (GRD, PCM, GES, ...) appear here automatically
 *	once they call cluster_shmem_register_region from their own init
 *	helper.
 *
 *	Column shape is stable from spec-1.3 onward; future amends may
 *	append columns at the tail (e.g. wait_time / hit_ratio for stage
 *	4+ performance monitoring) but not change/remove existing columns.
 *
 *	In --disable-cluster builds the SRF returns an empty result set,
 *	the same convention as the other cluster_get_* SRFs above.
 *
 *	See specs/spec-1.3-shmem-region-registry.md §2.3 (column contract)
 *	and docs/cluster-shmem-design.md §10 (diagnostic views).
 * ============================================================ */

PG_FUNCTION_INFO_V1(cluster_shmem_dump_regions);

Datum
cluster_shmem_dump_regions(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);

	CLUSTER_INJECTION_POINT("cluster-shmem-views-srf-entry");

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
		int idx = 0;
		ClusterShmemRegion region;

		while (cluster_shmem_iter_regions(&idx, &region)) {
			Datum values[4];
			bool nulls[4] = { false, false, false, false };

			values[0] = CStringGetTextDatum(region.name);
			values[1] = Int64GetDatum((int64)region.size_fn());
			values[2] = Int32GetDatum(region.lwlock_count);
			values[3] = CStringGetTextDatum(region.owner_subsys);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}
#endif

	return (Datum)0;
}
