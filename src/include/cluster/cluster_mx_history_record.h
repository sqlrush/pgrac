/*-------------------------------------------------------------------------
 * cluster_mx_history_record.h
 *    Private history record comparisons; acquisition confers authority.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MX_HISTORY_RECORD_H
#define CLUSTER_MX_HISTORY_RECORD_H

#include "cluster/cluster_mx_history_wire.h"
#include "cluster/cluster_savepoint_reader.h"

#ifdef USE_PGRAC_CLUSTER
typedef struct ClusterMxHistoryRecord {
	UndoRecordHeader header;
	ClusterSavepointReaderIdentity identity;
	ClusterSavepointReaderRecord payload;
	UndoSavepointRollbackPayload control;
	uint16 record_offset, record_length;
	uint8 record_class, effect;
	bool matches_target, canonical_generation_known;
} ClusterMxHistoryRecord;

/* request has passed the history codec. DATA/root/generation, retention and
 * admission belong to the original reader. Capture is provisional: V1 has
 * no FullXid, and an operation tail supplies only a compare challenge.
 * Outputs do not overlap inputs. Failure clears outputs. */
extern bool cluster_mx_history_record_capture(const ClusterMxHistoryRequest *request,
											  const char page[BLCKSZ], char record_out[BLCKSZ],
											  ClusterMxHistoryRecord *out);

/* record is an unmodified capture/recheck result kept in the reader's scope.
 * top is the independently sampled local canonical TT result, never a peer
 * result. This comparison grants neither ordering, a LIVE grant, visibility,
 * nor control completion/retirement. Missing original FullXid is UNKNOWN. */
extern bool cluster_mx_history_record_bind_top(const ClusterMxHistoryRequest *request,
											   const ClusterMxHistoryRecord *record,
											   const ClusterTxResolution *top,
											   ClusterSavepointReaderIdentity *out);

/* Compare the captured exact record with fresh DATA after the original TT
 * sample. Both sides use the same decoder; no new acquisition is performed.
 * LIVE->DISCARDED and legal forward control progress are the only mutations.
 * The returned control remains a snapshot, never a retirement permission. */
extern bool cluster_mx_history_record_recheck(const ClusterMxHistoryRequest *request,
											  const ClusterMxHistoryRecord *first,
											  const char *first_bytes, size_t first_length,
											  const char fresh[BLCKSZ], char record_out[BLCKSZ],
											  ClusterMxHistoryRecord *out);
#endif
#endif
