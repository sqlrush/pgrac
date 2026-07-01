/*-------------------------------------------------------------------------
 *
 * cluster_rfs.h
 *	  ADG Remote File Server (RFS) helpers.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_rfs.h
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RFS_H
#define CLUSTER_RFS_H

#include "access/xlogdefs.h"
#include "lib/stringinfo.h"

#ifdef USE_PGRAC_CLUSTER

extern void cluster_rfs_observe_received_chunk(const char *buf, Size nbytes, XLogRecPtr recptr,
											   uint16 *last_thread_id);
extern void cluster_rfs_append_reply_trailer(StringInfo reply_message, uint16 last_thread_id);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_RFS_H */
