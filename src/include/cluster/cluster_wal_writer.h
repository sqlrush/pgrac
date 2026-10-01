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
	XLogRecPtr complete_end;
} ClusterWalWriterFlushV1;

/* A native reservation observation, NOT a complete/durable prefix. Keep
 * one per background job after its directory cut. Only the original writer
 * may confirm it; callers must not construct it from a byte/LSN guess. */
typedef struct ClusterWalWriterSampleV1 {
	ClusterWalWriterToken writer;
	XLogRecPtr reserved_end;
} ClusterWalWriterSampleV1;

/* Nonblocking original-writer observations. Safe for LMON CONTROL dispatch
 * and native I/O background roles: no file I/O, force-flush, allocation or
 * native LWLock wait. Sample output is not itself a Flush promise. Confirm
 * preserves this job's exact end even if later insertions advance. A changed
 * full writer token is rejected, never adopted. Errors clear nonalias output;
 * overlapping input/output is rejected without modifying either. */
extern ClusterControlRootResult cluster_wal_writer_sample_v1(ClusterWalWriterSampleV1 *out);
extern ClusterControlRootResult
cluster_wal_writer_confirm_v1(const ClusterWalWriterSampleV1 *sample, ClusterWalWriterFlushV1 *out);

/* Background physical input only, not ROOT/recovery/GC permission. Native
 * Flush may bisect a later record. complete_end is sampled from the native
 * reservation owner and then confirmed by native Flush >= complete_end.
 * Until that is true this call returns RECONFIG_WAIT, with cleared output
 * and the same process-local reservation retained for the next poll. A
 * changed writer token discards that pending sample. New inserts cannot
 * perpetually move the pending cut forward. On success it is consumed;
 * it never forces flush or infers a complete cut by decoding stored bytes. */
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
