/*-------------------------------------------------------------------------
 * cluster_savepoint.h -- backend-local savepoint frame/name lifecycle.
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SAVEPOINT_H
#define CLUSTER_SAVEPOINT_H

#include "c.h"
#include "access/transam.h"

/* Called after the native frame is published; does not allocate an xid. */
extern void cluster_savepoint_start(SubTransactionId subid);

/* Publish a successfully allocated user name, or rebind a restarted point.
 * name_slot belongs to the native frame until cluster_savepoint_forget().
 * Older names at this savepoint level are retired; their frames stay live. */
extern void cluster_savepoint_define(SubTransactionId subid, char **name_slot,
									 int savepoint_level);

/* Native PopTransaction calls this for RELEASE and abort cleanup, including
 * an incomplete start. This only retires memory, not transaction effects. */
extern void cluster_savepoint_forget(SubTransactionId subid);

#endif
