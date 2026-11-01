/* Process-local composition assembly, not wire or transaction authority. */
#ifndef CLUSTER_MULTIXACT_COMPOSITION_H
#define CLUSTER_MULTIXACT_COMPOSITION_H

#include "cluster/cluster_multixact_current.h"

#define CLUSTER_MX_COMPOSITION_PAGE_MEMBERS 256

typedef enum ClusterMxCompositionReadStatus {
	MXCOMP_READ_OK = 0, MXCOMP_READ_RETRY, MXCOMP_READ_UNKNOWN
} ClusterMxCompositionReadStatus;

typedef enum ClusterMxCompositionReceiveResult {
	MXCOMP_UNKNOWN = 0, MXCOMP_PARTIAL, MXCOMP_COMPLETE
} ClusterMxCompositionReceiveResult;

typedef struct ClusterMxCompositionReceiveStats {
	uint64 accepted_frames;
	uint64 unknown_results;
	uint64 restart_requests;
} ClusterMxCompositionReceiveStats;

typedef struct ClusterMxCompositionReply {
	const void *source;
	Size source_bytes;
	uint32 total, first, count, next;
	uint32 cap; /* Local request bound, never a peer-supplied permission. */
	bool last;
	const ClusterCurrentMxMemberDesc *members;
	Size member_bytes, frame_bytes;
} ClusterMxCompositionReply;

typedef struct ClusterMxCompositionPage {
	struct ClusterMxCompositionPage *next;
	uint32 first, count;
	ClusterCurrentMxMemberDesc members[CLUSTER_MX_COMPOSITION_PAGE_MEMBERS];
} ClusterMxCompositionPage;

typedef struct ClusterMxCompositionReceive ClusterMxCompositionReceive;

/* source is the caller's complete, prevalidated semantic identity, never a
 * hash or raw MXID. Exact bytes are owned/copied; no wire layout is defined
 * here. G1 retention and G5 decoder/capability checks remain caller duties.
 * stats must outlive the operation, starts caller-zeroed and survives failure.
 * Only operation-local memory is acquired, under CurrentMemoryContext.
 */
extern bool cluster_mx_composition_begin(ClusterMxCompositionReceive **op,
	const void *source, Size source_bytes, uint32 total, ClusterMxCompositionReceiveStats *stats);
extern ClusterMxCompositionReceiveResult cluster_mx_composition_accept(
	ClusterMxCompositionReceive **op, ClusterMxCompositionReadStatus status,
	const ClusterMxCompositionReply *reply);
extern uint32 cluster_mx_composition_next(const ClusterMxCompositionReceive *op);
/* Provisional composition metadata only, never a visibility/conflict result.
 * NULL/zero until complete. Borrowed pages expire on accept, finish or parent
 * context reset; caller still owes all source/grant/tuple/fence final checks.
 */
extern const ClusterMxCompositionPage *cluster_mx_composition_pages(
	const ClusterMxCompositionReceive *op, uint32 *total);
extern void cluster_mx_composition_finish(ClusterMxCompositionReceive **op);

#endif
