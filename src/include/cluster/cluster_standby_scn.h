/*-------------------------------------------------------------------------
 *
 * cluster_standby_scn.h
 *	  ADG standby SCN tracker runtime facade.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_standby_scn.h
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_STANDBY_SCN_H
#define CLUSTER_STANDBY_SCN_H

#include "access/xlogdefs.h"
#include "cluster/cluster_scn.h"

#ifdef USE_PGRAC_CLUSTER

extern void cluster_standby_scn_mark_received(uint16 thread_id, XLogRecPtr receive_lsn);
extern void cluster_standby_scn_mark_applied(uint16 thread_id, XLogRecPtr apply_lsn);
extern void cluster_standby_scn_apply_barrier(uint16 thread_id, XLogRecPtr barrier_lsn,
											  SCN thread_safe_scn);
extern SCN cluster_standby_scn_consistent_scn(void);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_STANDBY_SCN_H */
