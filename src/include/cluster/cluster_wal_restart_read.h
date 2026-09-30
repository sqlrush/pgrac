/*-------------------------------------------------------------------------
 *
 * cluster_wal_restart_read.h
 *    Read-only physical access to the selected restart WAL generation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_wal_restart_read.h
 *
 * NOTES
 *    PGRAC-original interface. The caller retains the selected input; this
 *    opener grants neither writer permission nor recovery admission.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAL_RESTART_READ_H
#define CLUSTER_WAL_RESTART_READ_H

#include "cluster/cluster_wal_source.h"

/* OK_PRIMARY returns one owned O_RDONLY fd; every other result leaves -1.
 * ABSENT means only a missing segment in an otherwise validated namespace.
 * Native XLogReader remains responsible for record/page validation. */
extern ClusterControlRootResult
cluster_wal_restart_segment_open(const char *wal_root, const ClusterWalSourceRef *input,
								 TimeLineID timeline, XLogSegNo segno, int segsize, int *fd_out);

#endif /* CLUSTER_WAL_RESTART_READ_H */
