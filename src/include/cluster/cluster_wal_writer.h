/* PGRAC: native WAL writer ownership, never a persistent flush promise.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAL_WRITER_H
#define CLUSTER_WAL_WRITER_H
#include "cluster/cluster_wal_source.h"

typedef struct ClusterWalWriterToken {
	ClusterWalSourceRef ref;
	uint64 epoch;
	XLogRecPtr startup_first_lsn;
} ClusterWalWriterToken;

typedef struct ClusterWalWriterFlushV1 {
	ClusterWalWriterToken writer;
	XLogRecPtr flushed_end;
} ClusterWalWriterFlushV1;

/* Background physical input only, not ROOT/recovery/GC permission. Native
 * Flush may bisect a later record; only the bounded reader may round it. */
extern ClusterControlRootResult cluster_wal_writer_flushed_v1(ClusterWalWriterFlushV1 *out);

/* Snapshot before I/O, revalidate after fsync before exposing native Flush.
 * Both calls are nonblocking and perform no file I/O or allocation. */
extern ClusterControlRootResult cluster_wal_writer_begin(TimeLineID timeline,
														ClusterWalWriterToken *token);
extern ClusterControlRootResult cluster_wal_writer_check(const ClusterWalWriterToken *token);
extern ClusterControlRootResult cluster_wal_writer_ready(TimeLineID timeline);
/* StartupProcess only, outside native critical sections. The existing root
 * owner verifies actual EMPTY; pg_wal must name its exact successor. */
extern ClusterControlRootResult
cluster_wal_writer_startup_prepare(const ClusterControlRootIdentity *self,
								  const uint8 operation_uuid[16], XLogRecPtr *first_segment);
extern bool cluster_wal_writer_startup_matches(const ClusterControlRootIdentity *self,
											 const uint8 operation_uuid[16], XLogRecPtr first_segment);
#endif
