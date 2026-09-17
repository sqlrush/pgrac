/*-------------------------------------------------------------------------
 *
 * cluster_replacement_request.h
 *    Same-node replacement request marker (RPLM) exact codec.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_REPLACEMENT_REQUEST_H
#define CLUSTER_REPLACEMENT_REQUEST_H

#include "c.h"

#include "cluster/cluster_replacement_episode.h"


#define CLUSTER_VOTING_SLOT_FLAG_REPLACEMENT_REQUESTED UINT64_C(0x0000000000000004)
#define CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET 112
#define CLUSTER_REPLACEMENT_MARKER_BYTES 64
#define CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES 368
#define CLUSTER_REPLACEMENT_MARKER_MAGIC UINT32_C(0x52504c4d)
#define CLUSTER_REPLACEMENT_MARKER_VERSION UINT16_C(1)
#define CLUSTER_REPLACEMENT_MARKER_PHASE_REQUESTED UINT8_C(1)

typedef struct ClusterReplacementRequestMarker {
	uint32 magic;
	uint16 version;
	uint8 phase;
	uint8 reserved0;
	int32 target_node_id;
	uint32 reserved1;
	uint64 baseline_epoch;
	uint64 old_admitted_incarnation;
	uint64 fresh_incarnation;
	uint64 request_nonce;
	uint64 grammar_fingerprint;
	uint32 crc32c;
	uint32 reserved2;
} ClusterReplacementRequestMarker;

typedef enum ClusterReplacementRequestSlotState {
	CLUSTER_REPLACEMENT_REQUEST_SLOT_CLEAR = 0,
	CLUSTER_REPLACEMENT_REQUEST_SLOT_VALID = 1,
	CLUSTER_REPLACEMENT_REQUEST_SLOT_HOLD = 2
} ClusterReplacementRequestSlotState;

StaticAssertDecl(sizeof(ClusterReplacementRequestMarker) == CLUSTER_REPLACEMENT_MARKER_BYTES,
				 "replacement request marker host shape must remain 64 bytes");
StaticAssertDecl(CLUSTER_REPLACEMENT_MARKER_RESERVED1_OFFSET + CLUSTER_REPLACEMENT_MARKER_BYTES
					 <= CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES,
				 "replacement request marker must fit the voting-slot reserved1");

extern bool cluster_replacement_request_encode(const ClusterReplacementRequestMarker *marker,
											   uint8 out[CLUSTER_REPLACEMENT_MARKER_BYTES]);
extern bool cluster_replacement_request_decode(const uint8 bytes[CLUSTER_REPLACEMENT_MARKER_BYTES],
											   int32 expected_target_node,
											   ClusterReplacementRequestMarker *out);
extern bool
cluster_replacement_request_pack(uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES],
								 const ClusterReplacementRequestMarker *marker);
extern bool cluster_replacement_request_unpack(
	const uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES], int32 expected_target_node,
	ClusterReplacementRequestMarker *out);
extern void
cluster_replacement_request_clear(uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES]);
extern bool cluster_replacement_request_is_clear(
	const uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES]);
extern ClusterReplacementRequestSlotState cluster_replacement_request_slot_state(
	uint64 flags, const uint8 reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES],
	int32 expected_target_node, uint64 expected_slot_incarnation,
	ClusterReplacementRequestMarker *out);
extern ClusterReplacementRequestSlotState cluster_replacement_request_preserve_per_disk(
	uint64 prior_flags, const uint8 prior_reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES],
	int32 expected_target_node, uint64 expected_slot_incarnation, uint64 *new_flags,
	uint8 new_reserved1[CLUSTER_REPLACEMENT_REQUEST_RESERVED1_BYTES]);

#endif /* CLUSTER_REPLACEMENT_REQUEST_H */
