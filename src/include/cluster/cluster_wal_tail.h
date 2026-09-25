/* PGRAC: read-only, exact-generation recovery WAL observation.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#ifndef CLUSTER_WAL_TAIL_H
#define CLUSTER_WAL_TAIL_H

#include "cluster/cluster_wal_durable_prefix.h"

typedef struct ClusterWalTailObservation {
	ClusterWalDurablePrefix durable_prefix;
	XLogRecPtr complete_end;
	XLogRecPtr last_record_start;
	pg_crc32c last_record_crc;
	uint64 records;
} ClusterWalTailObservation;

/* Caller supplies a root-selected immutable reference and exact checkpoint
 * record start, NOT an arbitrary point at which to search for a later record.
 * This validates physical input only: the owner must hold/revalidate isolation,
 * retention and recovery serialization before consuming or publishing it.
 * No replay, authority publication, local-timeline or legacy-path fallback.
 * All outputs are cleared on refusal; ERROR/cancellation releases owned FDs.
 */
extern ClusterControlRootResult cluster_wal_tail_observe(const char *wal_root,
														 const ClusterWalDurablePrefixRef *ref,
														 int segment_size, XLogRecPtr scan_lower,
														 XLogRecPtr minimum_end,
														 ClusterWalTailObservation *out);

#endif /* CLUSTER_WAL_TAIL_H */
