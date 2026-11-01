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
	uint32 total, first, count, next; /* total/first/next are physical positions. */
	uint32 cap; /* Local request bound, never a peer-supplied permission. */
	bool last;
	bool skipped_zero;
	const ClusterCurrentMxMemberDesc *members;
	Size member_bytes, frame_bytes;
} ClusterMxCompositionReply;

/* Native portion of source identity, never a wire/capability or a substitute
 * for the original caller's service/storage/H scope and final checks. */
typedef struct ClusterMxCompositionNativeSource {
	uint64 epoch;
	int32 origin_node;
	MultiXactId multi, oldest_multi;
	MultiXactOffset oldest_offset, start;
	uint32 span;
} ClusterMxCompositionNativeSource;

typedef struct ClusterMxCompositionNativeChunk {
	ClusterMxCompositionNativeSource source;
	uint32 first, next, count;
	bool last, skipped_zero;
	ClusterCurrentMxMemberDesc members[CLUSTER_MX_COMPOSITION_PAGE_MEMBERS];
} ClusterMxCompositionNativeChunk;

extern ClusterMxCompositionReadStatus cluster_mx_composition_read_native(
	MultiXactId multi, const ClusterMxCompositionNativeSource *expected,
	uint32 first, uint32 cap, ClusterMxCompositionNativeChunk *out);

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
 * total is physical span L; start must be bound inside the same source.
 * next() advances physically, while pages().first and output total are logical.
 * A zero-only physical tail is accepted only as the one verified slot-0 skip.
 * Only operation-local memory is acquired, under CurrentMemoryContext.
 */
extern bool cluster_mx_composition_begin(ClusterMxCompositionReceive **op,
	const void *source, Size source_bytes, uint32 total, MultiXactOffset start,
	ClusterMxCompositionReceiveStats *stats);
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
