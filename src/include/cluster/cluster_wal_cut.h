/* PGRAC: original-writer background read cuts, never DATA/ROOT authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAL_CUT_H
#define CLUSTER_WAL_CUT_H

#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_wal_writer.h"

#define CLUSTER_WAL_CUT_BYTES 200
#define CLUSTER_WAL_CUT_SAMPLE 1
#define CLUSTER_WAL_CUT_SAMPLED 2
#define CLUSTER_WAL_CUT_CONFIRM 3
#define CLUSTER_WAL_CUT_CONFIRMED 4

typedef struct ClusterWalCutMessageV1 {
	uint32 verb;
	uint32 collector;
	uint64 nonce;
	uint64 collector_incarnation;
	uint64 epoch;
	ClusterWalSourceRef source;
	uint64 native_config_generation;
	XLogRecPtr reserved_end;
	XLogRecPtr flushed_end;
} ClusterWalCutMessageV1;

typedef struct ClusterWalCutV1 ClusterWalCutV1;

extern bool cluster_wal_cut_encode_v1(const ClusterWalCutMessageV1 *message,
									  uint8 bytes[CLUSTER_WAL_CUT_BYTES]);
extern bool cluster_wal_cut_decode_v1(const void *bytes, Size length, ClusterWalCutMessageV1 *out);

/* Invoke after the job's exact directory cut. One bounded mailbox serializes
 * pending background observations at this postmaster; capacity returns WAIT.
 * Each handle keeps its accepted end fixed. A proposal without original native
 * Flush coverage cannot escape poll as success. The original ROOT/WALR scope
 * and a physical decode through this end remain mandatory. No source-side job,
 * force-flush, retention grant or replay permission is created.
 *
 * Handles are process/ResourceOwner local. Release all handles explicitly;
 * ResourceOwner/child-exit cleanup cancels a still-pending observation. */
extern ClusterControlRootResult cluster_wal_cut_begin_v1(const ClusterWalSourceRef *source,
														 ClusterWalCutV1 **out);
extern ClusterControlRootResult cluster_wal_cut_poll_v1(ClusterWalCutV1 *cut,
														ClusterWalWriterFlushV1 *out);
extern void cluster_wal_cut_release_v1(ClusterWalCutV1 **cut);

/* Existing CONTROL owner only; no CF, WAL I/O, native LWLock wait or SQL. */
extern void cluster_wal_cut_ingress_v1(const ClusterICEnvelope *env, const void *payload);
extern void cluster_wal_cut_lmon_tick_v1(void);
extern void cluster_wal_cut_register_v1(void);
extern void cluster_wal_cut_shmem_register_v1(void);

#endif
