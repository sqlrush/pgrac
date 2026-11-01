/* Complete composition metadata. No transaction verdict or heap grant. */
#ifndef CLUSTER_MULTIXACT_RANGE_H
#define CLUSTER_MULTIXACT_RANGE_H

#include "cluster/cluster_multixact_range_wire.h"
#include "cluster/cluster_ic_envelope.h"
#include "storage/buf_internals.h"
#include "utils/snapshot.h"

#define CLUSTER_MX_RANGE_REQUEST_KIND CLUSTER_FOREIGN_MX_GCS_RANGE_REQUEST_KIND_V1
#define CLUSTER_MX_RANGE_RESPONSE_KIND CLUSTER_FOREIGN_MX_GCS_RANGE_REPLY_STATUS_V1

typedef struct ClusterMxRangeSnapshot {
	SCN read_scn;
	uint64 read_epoch;
	uint32 snapshot_type, xmin, xmax, curcid;
} ClusterMxRangeSnapshot;

typedef struct ClusterMxRangeSource {
	ClusterMxCompositionNativeSource native;
	uint64 system_identifier, formation_epoch, boot_incarnation, admission_revision;
	ClusterMxRangeSnapshot snapshot;
	BufferTag target;
	uint32 capability_generation;
	uint64 reserved;
} ClusterMxRangeSource;

/* Independent from the legacy GCS/V5 prefixes. CRC covers this header with
 * checksum zero, and every body byte. No native pointer is transported. */
typedef struct ClusterMxRangeRoute {
	uint32 format_id, version, wire_bytes, kind;
	uint64 request_id, epoch;
	int32 requester_node, requester_backend;
	uint32 checksum, reserved;
} ClusterMxRangeRoute;

typedef struct ClusterMxRangeRequest {
	ClusterMxRangeRoute route;
	ClusterCurrentMxKey key;
	BufferTag target;
	uint32 first, cap, peer_generation, has_source, reserved;
	ClusterMxRangeSnapshot snapshot;
	ClusterMxRangeSource source;
} ClusterMxRangeRequest;

typedef struct ClusterMxRangeResponse {
	ClusterMxRangeRoute route;
	PGAlignedBlock data;
} ClusterMxRangeResponse;

/* The existing kind=2 body uses this independent variable-length request.
 * Only route.wire_bytes are transported; no padding/tail is sent. */
typedef struct ClusterMxProofRequest {
	ClusterMxRangeRoute route;
	PGAlignedBlock data;
} ClusterMxProofRequest;

/* Process-local copied request, not storage/TT/grant/admission authority. */
typedef struct ClusterMxProofRequestView {
	ClusterMxRangeRoute route;
	ClusterCurrentMxKey key;
	ClusterMxRangeSource source;
	uint32 peer_generation, total, first, count, next;
	ClusterMxRangeProofAsk asks[CLUSTER_MX_RANGE_PROOF_ASKS_PER_FRAME];
} ClusterMxProofRequestView;

StaticAssertDecl(sizeof(ClusterMxRangeSnapshot) == 32, "range snapshot size");
StaticAssertDecl(sizeof(ClusterMxRangeSource) == 128, "range source size");
StaticAssertDecl(sizeof(ClusterMxRangeRoute) == 48, "range route size");
StaticAssertDecl(sizeof(ClusterMxRangeRequest) == 264, "range request size");
StaticAssertDecl(offsetof(ClusterMxRangeResponse, data) == 48, "aligned range reply");
StaticAssertDecl(offsetof(ClusterMxProofRequest, data) == 48, "aligned proof request");

/* Structural packet codecs, without allocation, transport or CTRC touch.
 * Pack failures preserve destination bytes and return bytes=0. Decode failure
 * clears the entire view. loaded_catalog and peer_capabilities must come from
 * the original loaded control/negotiated peer, never the incoming packet.
 * peer_generation is a requester-side echo, not the holder's own connection
 * generation. The original service must still hold/revalidate its KO/CONTROL
 * admission and native H/source/TT/grants through the final checks. */
extern bool cluster_mx_proof_request_pack(const ClusterMxRangeWireContext *ctx,
	int32 requester_node, int32 requester_backend, const void *body, uint32 body_bytes,
	ClusterMxProofRequest *request, uint32 capacity, uint32 *bytes);
extern bool cluster_mx_proof_request_decode(const void *wire, uint32 bytes,
	uint32 loaded_catalog, uint32 peer_capabilities, ClusterMxProofRequestView *view);
extern bool cluster_mx_proof_response_pack(const ClusterMxProofRequestView *request,
	const ClusterMxRangeWireContext *ctx, ClusterMxCompositionReadStatus status,
	const ClusterMxProofReply *reply, ClusterMxRangeResponse *response,
	uint32 capacity, uint32 *bytes);
/* Structural binding only. A valid UNKNOWN/RETRY is transported as such. */
extern bool cluster_mx_proof_response_matches(const ClusterMxProofRequestView *request,
	const void *wire, uint32 bytes, uint32 loaded_catalog, uint32 peer_capabilities,
	uint16 expected_origin);

typedef struct ClusterMxProofTransportStats {
	uint64 attempts, enqueued, replies, request_bytes, reply_bytes;
} ClusterMxProofTransportStats;

/* One remote frame, using the original FULL_MX slot and exit cleanup. The
 * caller splits contiguous windows by actual member origin before this call;
 * the MXID/source origin need not be that origin. The stripe lookup is routing
 * only, never FullXid/TT authority. Caller-sampled peer_generation must match
 * that member origin's current negotiated record. Rewrites both request ids
 * after reserving the slot. Counts actual enqueue attempts, successful queued
 * bytes and returned frames; no implicit retry or elapsed-wait SQL error.
 * false/ERROR leaves response zero and bytes=0. Even true supplies provisional
 * bytes only: decode the result, retain the original operation/read scope and
 * revalidate canonical proofs/grants/tuple/fence before consuming them.
 * Local origins use their original owner, not a network round trip here. */
extern bool cluster_gcs_current_mx_proofs_fetch_and_wait(int32 origin,
	ClusterMxProofRequest *request, uint32 request_bytes, ClusterMxRangeResponse *response,
	uint32 *bytes, ClusterMxProofTransportStats *stats);

extern void cluster_mx_range_request_seal(ClusterMxRangeRequest *request);
extern void cluster_mx_range_response_seal(ClusterMxRangeResponse *response, uint32 body_bytes);
extern bool cluster_mx_range_refuse(const ClusterMxRangeRequest *request, uint32 loaded_catalog,
	ClusterMxRangeResponse *response, uint32 capacity, uint32 *bytes);
extern bool cluster_mx_range_transport_route(const void *wire, uint32 bytes,
	uint8 message_type, ClusterMxRangeRoute *route, BufferTag *tag);
extern bool cluster_mx_range_transport_candidate(const void *wire, uint32 bytes,
	uint8 message_type);
extern bool cluster_mx_range_serve(const ClusterMxRangeRequest *request,
	ClusterMxRangeResponse *response, uint32 capacity, uint32 *bytes);
extern bool cluster_gcs_current_mx_range_fetch_and_wait(ClusterMxRangeRequest *request,
	ClusterMxRangeResponse *response, uint32 *bytes);
extern void cluster_gcs_current_mx_range_serve_inline(const ClusterICEnvelope *env,
	const void *payload);

/* Caller owns the original tuple/fence and snapshot. Complete metadata stays
 * provisional until those owners and the canonical member proofs revalidate.
 * A non-OK frame returns false, discards the entire operation and never spins.
 * The caller's ResourceOwner also revokes all borrowed pages at release. */
extern bool cluster_multixact_current_describe_full(const ClusterCurrentMxKey *key,
	const BufferTag *target, const ClusterMxRangeSnapshot *snapshot,
	ClusterMxCompositionReceive **op, ClusterMxCompositionReceiveStats *stats);
extern void cluster_multixact_current_describe_finish(ClusterMxCompositionReceive **op);
/* Same original owner and qualification as describe_full/proofs_accept. Encode
 * only the next unaccepted window, without sending or entering CTRC touch.
 * Context key/catalog/peer generation and exact source must match the original
 * operation; scope is rechecked before and after encoding. Failure retires the
 * operation, leaves wire unchanged and bytes zero; ERROR cleans up and rethrows.
 * The future transport must route/split by real member origin and account for
 * every attempt; this entry does not choose origin or reserve G4 capacity.
 */
extern bool cluster_multixact_current_proofs_request(ClusterMxCompositionReceive **op,
	const ClusterMxRangeWireContext *ctx, void *wire, uint32 capacity, uint32 *bytes);
/* Keep the original ResourceOwner/directory borrow in sync on proof failure
 * and ERROR, rechecking requester epoch/storage/CONTROL before and after each
 * provisional window. This does not validate member grants or tuple fences.
 * Registered operations must not use the raw G2 accept entry. */
extern ClusterMxCompositionReceiveResult
cluster_multixact_current_proofs_accept(ClusterMxCompositionReceive **op,
										ClusterMxCompositionReadStatus status,
										const ClusterMxProofReply *reply);
extern ClusterMxDescribeResult cluster_multixact_current_describe_scoped(
	const ClusterCurrentMxKey *key, const BufferTag *target, Snapshot snapshot,
	ClusterCurrentMxMemberDesc *members, uint16 cap, uint16 *count, uint32 *total);

#endif
