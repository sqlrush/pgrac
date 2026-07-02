/*-------------------------------------------------------------------------
 *
 * cluster_mrp_apply.h
 *	  ADG Managed Recovery Process apply helpers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_mrp_apply.h
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MRP_APPLY_H
#define CLUSTER_MRP_APPLY_H

#include "access/xlogdefs.h"
#include "cluster/cluster_scn.h"

#ifdef USE_PGRAC_CLUSTER

extern bool cluster_mrp_apply_record_replayed(uint16 thread_id, XLogRecPtr apply_lsn,
											  SCN record_scn);
extern bool cluster_mrp_apply_barrier_replayed(uint16 thread_id, XLogRecPtr barrier_lsn,
											   SCN thread_safe_scn, uint16 primary_thread_count);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_MRP_APPLY_H */
