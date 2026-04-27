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
 *	  pg_stat_cluster_wait_events view.  It iterates the 46 cluster wait
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
#include "utils/wait_event.h"

#include "cluster/cluster_views.h"


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
};

/* Compile-time assertion: array length must match the documented count. */
StaticAssertDecl(lengthof(cluster_wait_event_infos) == CLUSTER_WAIT_EVENTS_COUNT,
				 "cluster_wait_event_infos length must equal CLUSTER_WAIT_EVENTS_COUNT");

#endif /* USE_PGRAC_CLUSTER */


Datum
cluster_get_wait_events(PG_FUNCTION_ARGS)
{
	/*
	 * Use PG's helper to set up a tuplestore-returning SRF.  This pattern
	 * matches pg_stat_get_subscription_stats and similar PG core SRFs.
	 */
	InitMaterializedSRF(fcinfo, 0);

#ifdef USE_PGRAC_CLUSTER
	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

		for (int i = 0; i < CLUSTER_WAIT_EVENTS_COUNT; i++)
		{
			uint32		info = cluster_wait_event_infos[i];
			Datum		values[2];
			bool		nulls[2] = {false, false};
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
	return (Datum) 0;
}
