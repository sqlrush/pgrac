/*-------------------------------------------------------------------------
 * cluster_mx_history.h
 *    Backend-private capture for exact MultiXact history collection.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MX_HISTORY_H
#define CLUSTER_MX_HISTORY_H
#include "c.h"
#if defined(USE_PGRAC_CLUSTER) && !defined(FRONTEND)
#include "access/htup_details.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_mx_history_wire.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_semantic_activation.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

typedef struct ClusterMxHistoryCollector ClusterMxHistoryCollector;
/* A copy of captured inputs, never a proof or retention permission. */
typedef struct ClusterMxHistoryCapture {
	ClusterPcmOwnSnapshot pcm;
	XLogRecPtr page_lsn;
	ItemPointerData tid;
	ItemIdData line_pointer;
	uint16 tuple_length;
	uint8 tuple_header[SizeofHeapTupleHeader];
	ClusterItlSlotData roots[CLUSTER_ITL_INITRANS_DEFAULT];
	Oid relation_id;
	Buffer buffer;
	ClusterCurrentMxKey mxkey;
	ClusterCurrentMxMemberDesc members[CLUSTER_CURRENT_MX_MAX_MEMBERS];
	uint16 nmembers;
	bool read_only;
	ClusterSemanticAdmissionToken admission;
	ClusterR4CrOwnerChallenge requester_owner;
	SCN read_scn, retained_floor;
	uint64 read_epoch, snapshot_identity, descriptor_hash;
	uint64 collector_id, capture_generation, backend_incarnation;
} ClusterMxHistoryCapture;

/* Caller retains its relation lock and buffer pin. Begin/recheck require the
 * original content lock and must be outside critical sections. A single
 * capture per backend owns one reference to the actual retained Snapshot. */
extern bool cluster_mx_history_capture_begin(Relation rel, Buffer buffer, HeapTuple tuple,
											 Snapshot actual, const ClusterCurrentMxKey *mxkey,
											 const ClusterCurrentMxMemberDesc *members,
											 uint16 nmembers,
											 const ClusterSemanticAdmissionToken *admission,
											 ClusterMxHistoryCollector **out);
/* Refreshes native scope/admission/owner evidence; failure zeroes out and
 * cancels this capture. No DATA request or proof is produced by this API. */
extern bool cluster_mx_history_capture_read(ClusterMxHistoryCollector *collector,
											ClusterMxHistoryCapture *out);
/* Full relock comparison, including descriptor bytes, not just their hash.
 * False invalidates the entire capture. This does not complete collection. */
extern bool cluster_mx_history_capture_recheck(ClusterMxHistoryCollector *collector, Relation rel,
											   Buffer buffer, HeapTuple tuple,
											   const ClusterCurrentMxKey *mxkey,
											   const ClusterCurrentMxMemberDesc *members,
											   uint16 nmembers);
/* Idempotent until TopTransactionContext resets. No waiting or retirement.
 * The original ResourceOwner BEFORE_LOCKS/exit callbacks also cancel it. */
extern void cluster_mx_history_cancel(ClusterMxHistoryCollector *collector);
#endif
#endif
