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

StaticAssertDecl(sizeof(ClusterMxRangeSnapshot) == 32, "range snapshot size");
StaticAssertDecl(sizeof(ClusterMxRangeSource) == 128, "range source size");
StaticAssertDecl(sizeof(ClusterMxRangeRoute) == 48, "range route size");
StaticAssertDecl(sizeof(ClusterMxRangeRequest) == 264, "range request size");
StaticAssertDecl(offsetof(ClusterMxRangeResponse, data) == 48, "aligned range reply");

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
/* Keep the original ResourceOwner/directory borrow in sync on proof failure
 * and ERROR. Registered operations must not use the raw G2 accept entry. */
extern ClusterMxCompositionReceiveResult
cluster_multixact_current_proofs_accept(ClusterMxCompositionReceive **op,
										ClusterMxCompositionReadStatus status,
										const ClusterMxProofReply *reply);
extern ClusterMxDescribeResult cluster_multixact_current_describe_scoped(
	const ClusterCurrentMxKey *key, const BufferTag *target, Snapshot snapshot,
	ClusterCurrentMxMemberDesc *members, uint16 cap, uint16 *count, uint32 *total);

#endif
