/*-------------------------------------------------------------------------
 *
 * cluster_mrp_apply.c
 *	  ADG Managed Recovery Process apply helpers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_mrp_apply.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_mrp.h"
#include "cluster/cluster_mrp_apply.h"
#include "cluster/cluster_standby_scn.h"

bool
cluster_mrp_apply_record_replayed(uint16 thread_id, XLogRecPtr apply_lsn, SCN record_scn)
{
	if (!cluster_mrp_should_start())
		return true;
	if (!cluster_mrp_apply_master_can_apply())
		return false;

	cluster_standby_scn_mark_applied(thread_id, apply_lsn);
	cluster_mrp_publish_watermarks(apply_lsn, apply_lsn,
								   (uint64)cluster_standby_scn_consistent_scn());
	(void)record_scn; /* observed by redo-side SCN code; read floor is barrier-only. */
	return true;
}

bool
cluster_mrp_apply_barrier_replayed(uint16 thread_id, XLogRecPtr barrier_lsn, SCN thread_safe_scn,
								   uint16 primary_thread_count)
{
	if (!cluster_mrp_should_start())
		return true;
	if (!cluster_mrp_apply_master_can_apply())
		return false;

	cluster_standby_scn_apply_barrier(thread_id, barrier_lsn, thread_safe_scn,
									  primary_thread_count);
	return true;
}

#endif /* USE_PGRAC_CLUSTER */
