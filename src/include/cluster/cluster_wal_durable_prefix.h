/*-------------------------------------------------------------------------
 * PGRAC: exact per-writer durable WAL prefix representation.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_WAL_DURABLE_PREFIX_H
#define CLUSTER_WAL_DURABLE_PREFIX_H

#include "access/xlogdefs.h"
#include "cluster/cluster_wal_claim.h"
#include "port/pg_crc32c.h"

#define CLUSTER_WAL_DURABLE_PREFIX_BYTES 256

typedef struct ClusterWalDurablePrefixRef {
	ClusterWalThreadClaimRefV2 claim;
	TimeLineID timeline;
} ClusterWalDurablePrefixRef;

/* Logical values, never an on-disk struct overlay or a flush/admission proof. */
typedef struct ClusterWalDurablePrefix {
	uint64 sequence;
	XLogRecPtr exclusive_end;
	XLogRecPtr record_start;
	pg_crc32c record_crc;
} ClusterWalDurablePrefix;

extern ClusterControlRootResult
cluster_wal_durable_prefix_encode(const ClusterWalDurablePrefixRef *ref,
								  const ClusterWalDurablePrefix *prefix,
								  uint8 bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES]);
extern ClusterControlRootResult
cluster_wal_durable_prefix_decode(const uint8 *bytes, size_t len,
								  const ClusterWalDurablePrefixRef *ref,
								  ClusterWalDurablePrefix *out);
/* Both logical values must belong to this exact decoded reference. This only
 * checks monotonic representation; it authorizes neither publication nor ACK. */
extern ClusterControlRootResult
cluster_wal_durable_prefix_successor(const ClusterWalDurablePrefixRef *ref,
									 const ClusterWalDurablePrefix *previous,
									 const ClusterWalDurablePrefix *next);
/* Exact current only: never synthesize EMPTY or fall back to a backup. Caller
 * owns root/claim qualification, isolated-tail/WAL checks and revalidation.
 * A read is not evidence that this process flushed the file or its WAL. */
extern ClusterControlRootResult
cluster_wal_durable_prefix_read(const char *wal_root, const ClusterWalDurablePrefixRef *ref,
								ClusterWalDurablePrefix *out);

#ifndef FRONTEND
/* Native ordinary-writer adapter. Init is process-local and outside critical
 * sections. Publish is only valid inside XLogWrite's exclusive WALWriteLock;
 * the result gates native Flush visibility, never grants writer admission. */
extern void cluster_wal_durable_publish_init(void);
extern ClusterControlRootResult cluster_wal_durable_publish(TimeLineID timeline,
															XLogRecPtr physical_flush,
															XLogRecPtr previous_flush,
															XLogRecPtr *covered);
#endif

#endif /* CLUSTER_WAL_DURABLE_PREFIX_H */
