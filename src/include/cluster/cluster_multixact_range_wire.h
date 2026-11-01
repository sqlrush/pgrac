/* Bounded foreign-MX composition metadata and proof-request frames. */
#ifndef CLUSTER_MULTIXACT_RANGE_WIRE_H
#define CLUSTER_MULTIXACT_RANGE_WIRE_H

#include "cluster/cluster_foreign_mx_format.h"
#include "cluster/cluster_multixact_composition.h"

#define CLUSTER_MX_RANGE_REPLY 1
#define CLUSTER_MX_RANGE_PROOF_ASKS 2
#define CLUSTER_MX_RANGE_PROOFS 3
#define CLUSTER_MX_RANGE_LAST 1
#define CLUSTER_MX_RANGE_SKIPPED_ZERO 2
#define CLUSTER_MX_RANGE_PROOF_ASKS_PER_FRAME 7

/* Inputs must be sampled/validated by the original caller: loaded control,
 * one CONTROL connection generation, and its complete protected source.
 * These fields and codec OK do not establish storage or visibility authority.
 * The caller still owes original capability/epoch/grant/tuple/fence checks.
 * requested_cap is a local physical range bound, never a peer permission.
 */
typedef struct ClusterMxRangeWireContext {
	uint64 request_id, epoch;
	ClusterCurrentMxKey key;
	uint32 format_id, version, loaded_catalog, peer_capabilities, peer_generation;
	uint32 requested_cap;
	const void *source;
	Size source_bytes;
} ClusterMxRangeWireContext;

typedef struct ClusterMxRangeWireHeader {
	uint32 format_id, version, wire_length, source_bytes;
	uint64 request_id, epoch;
	ClusterCurrentMxKey key;
	uint32 peer_generation, total, first, count, next;
	uint8 kind, result, flags, reserved8;
	uint32 reserved32[2];
} ClusterMxRangeWireHeader;

typedef struct ClusterMxRangeProofAsk {
	TransactionId xid;
	uint32 member_ordinal;
	uint8 member_status, reserved[3];
} ClusterMxRangeProofAsk;

StaticAssertDecl(sizeof(ClusterMxRangeWireHeader) == 80, "range header must be 80 bytes");
StaticAssertDecl(sizeof(ClusterMxRangeProofAsk) == 12, "proof ask must be 12 bytes");

/* Encoders only modify the buffer on success; bytes is zero on failure.
 * reply.frame_bytes is receive-side metadata, computed by the decoder.
 * Decoding requires MAXALIGN payload storage. Borrowed source/members expire
 * with that input frame; G2 must copy them before the frame is reused.
 * Invalid/non-OK range frames clear the entire reply. UNKNOWN never supplies
 * positive visibility/conflict/reclamation information. No allocation/RPC.
 */
extern bool cluster_mx_range_wire_encode_reply(const ClusterMxRangeWireContext *ctx,
	ClusterMxCompositionReadStatus status, const ClusterMxCompositionReply *reply,
	void *wire, uint32 capacity, uint32 *bytes);
extern ClusterMxCompositionReadStatus cluster_mx_range_wire_decode_reply(
	const void *wire, uint32 bytes, const ClusterMxRangeWireContext *ctx,
	ClusterMxCompositionReply *reply);
extern bool cluster_mx_range_wire_encode_proof_asks(const ClusterMxRangeWireContext *ctx,
	uint32 total, uint32 first, const ClusterCurrentMxMemberDesc *members, uint32 count,
	void *wire, uint32 capacity, uint32 *bytes);
/* Encode the next unaccepted proof window of the complete original operation.
 * Source bytes must match exactly. requested_cap is in 1..7; positions and N
 * are logical uint32, independently of the native physical slot-0 skip.
 * This does not advance, allocate, send or touch proofs. Only accept_proofs
 * advances the window. Failure leaves wire unchanged and bytes zero. Raw
 * operations only: describe_full borrowers use the ResourceOwner-aware
 * cluster_multixact_current_proofs_request entry.
 */
extern bool cluster_mx_composition_encode_proof_asks(const ClusterMxCompositionReceive *op,
	const ClusterMxRangeWireContext *ctx, void *wire, uint32 capacity, uint32 *bytes);
/* asks_cap must be in 1..7. Failure zeroes asks/count/total, with no partial
 * proof request. These are requests, never canonical TT proofs or grants. */
extern bool cluster_mx_range_wire_decode_proof_asks(const void *wire, uint32 bytes,
	const ClusterMxRangeWireContext *ctx, ClusterMxRangeProofAsk *asks, uint32 asks_cap,
	uint32 *count, uint32 *total);

/* Structural/request binding only, never canonical freshness or a CTRC grant.
 * ACTIVE carrier bytes must still pass the original receiver's V2 match. */
extern bool cluster_mx_range_proof_entry_valid(const ClusterMxFullMemberProof *entry,
											   uint32 ordinal, uint32 local,
											   const ClusterCurrentMxMemberDesc *member);
extern bool cluster_mx_range_wire_encode_proofs(const ClusterMxRangeWireContext *ctx,
												ClusterMxCompositionReadStatus status,
												const ClusterMxProofReply *reply, void *wire,
												uint32 capacity, uint32 *bytes);
extern ClusterMxCompositionReadStatus cluster_mx_range_wire_decode_proofs(
	const void *wire, uint32 bytes, const ClusterMxRangeWireContext *ctx, uint16 expected_origin,
	uint32 total, uint32 first, const ClusterCurrentMxMemberDesc *members, uint32 count,
	ClusterMxProofReply *reply);

#endif
