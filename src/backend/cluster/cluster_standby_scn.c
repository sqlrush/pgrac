/*-------------------------------------------------------------------------
 *
 * cluster_standby_scn.c
 *	  ADG standby SCN tracker runtime facade.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_standby_scn.c
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_mrp.h"
#include "cluster/cluster_standby_scn.h"

void
cluster_standby_scn_mark_received(uint16 thread_id, XLogRecPtr receive_lsn)
{
	cluster_mrp_mark_thread_received(thread_id, receive_lsn);
}

void
cluster_standby_scn_mark_applied(uint16 thread_id, XLogRecPtr apply_lsn)
{
	cluster_mrp_mark_thread_applied(thread_id, apply_lsn);
}

void
cluster_standby_scn_apply_barrier(uint16 thread_id, XLogRecPtr barrier_lsn, SCN thread_safe_scn,
								  uint16 primary_thread_count)
{
	cluster_mrp_apply_thread_barrier(thread_id, barrier_lsn, thread_safe_scn, primary_thread_count);
}

SCN
cluster_standby_scn_consistent_scn(void)
{
	return cluster_mrp_standby_consistent_scn();
}

#endif /* USE_PGRAC_CLUSTER */
