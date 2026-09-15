/*-------------------------------------------------------------------------
 *
 * cluster_replacement_wire.h
 *    Spec-5.15A opcode-18 replacement episode wire codec.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_REPLACEMENT_WIRE_H
#define CLUSTER_REPLACEMENT_WIRE_H

#include "c.h"

#include "cluster/cluster_ic_envelope.h"
#include "cluster/storage/cluster_undo_block0.h"


#define GES_REQ_OPCODE_REPLACEMENT_EPISODE UINT32_C(18)
#define CLUSTER_REPLACEMENT_WIRE_VERSION UINT32_C(1)
#define CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_REQUEST UINT32_C(1)
#define CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_ACK UINT32_C(2)
#define CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY UINT32_C(3)
#define CLUSTER_REPLACEMENT_WIRE_PHASE_FAILSTOP_APPLICATION_ACK UINT32_C(4)
#define CLUSTER_REPLACEMENT_WIRE_BYTES 72
#define CLUSTER_REPLACEMENT_WIRE_BITMAP_BYTES 16
#define CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY 8
#define CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT \
	UINT64_C(0x8e0dae5b428905e4)


typedef struct ClusterReplacementWirePhase3 {
	uint64 jcmk_generation;
	uint32 episode_state_generation;
	uint32 reserved;
} ClusterReplacementWirePhase3;

StaticAssertDecl(sizeof(ClusterReplacementWirePhase3) == 16,
				 "replacement wire phase-3 body must remain 16 bytes");

typedef union ClusterReplacementWireBody {
	uint8 bitmap[CLUSTER_REPLACEMENT_WIRE_BITMAP_BYTES];
	ClusterReplacementWirePhase3 phase3;
} ClusterReplacementWireBody;

StaticAssertDecl(sizeof(ClusterReplacementWireBody) == 16,
				 "replacement wire phase body must remain 16 bytes");

/* Decoded host-order value.  Phase selects the meanings documented by
 * spec-5.15A §2.3 for target/epoch/identity0/identity1/body. */
typedef struct ClusterReplacementWireMessage {
	uint32 phase;
	int32 target_node_id;
	uint64 epoch;
	uint64 request_nonce;
	uint64 identity0;
	uint64 identity1;
	ClusterReplacementWireBody body;
	uint64 grammar_fingerprint;
} ClusterReplacementWireMessage;

/* The authenticated IC handler may only hand an observation to the formation
 * LMON.  It cannot apply JCMK, D13, membership, or admission state itself. */
typedef struct ClusterReplacementPhase3HandoffItem {
	ClusterReplacementWireMessage message;
	int32 authenticated_source_node_id;
	int32 local_receiver_node_id;
	uint32 control_connection_generation;
	uint32 reserved;
} ClusterReplacementPhase3HandoffItem;

typedef struct ClusterReplacementPhase3Handoff {
	uint64 producer_seq;
	uint64 consumer_seq;
	ClusterReplacementPhase3HandoffItem
		items[CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY];
} ClusterReplacementPhase3Handoff;

typedef enum ClusterReplacementPhase3IngressResult {
	CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED = 0,
	CLUSTER_REPLACEMENT_PHASE3_INGRESS_ENQUEUED,
	CLUSTER_REPLACEMENT_PHASE3_INGRESS_FULL
} ClusterReplacementPhase3IngressResult;


extern bool cluster_replacement_wire_encode(
	const ClusterReplacementWireMessage *message,
	uint8 out[CLUSTER_REPLACEMENT_WIRE_BYTES]);
extern bool cluster_replacement_wire_encode_phase3_snapshot(
	const ClusterR4PrerequisiteSnapshot *snapshot,
	uint8 out[CLUSTER_REPLACEMENT_WIRE_BYTES]);
extern bool cluster_replacement_wire_decode(
	const uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES],
	ClusterReplacementWireMessage *out);
extern void cluster_replacement_phase3_handoff_init(
	ClusterReplacementPhase3Handoff *handoff);
extern uint32 cluster_replacement_phase3_handoff_pending(
	const ClusterReplacementPhase3Handoff *handoff);
extern bool cluster_replacement_phase3_handoff_poll(
	ClusterReplacementPhase3Handoff *handoff,
	ClusterReplacementPhase3HandoffItem *out);
extern ClusterReplacementPhase3IngressResult
cluster_replacement_wire_phase3_ingress(
	ClusterReplacementPhase3Handoff *handoff, const ClusterICEnvelope *env,
	const void *payload, uint32 payload_length, int32 authenticated_source_node_id,
	int32 local_receiver_node_id, uint64 current_epoch,
	uint32 control_connection_generation);

/* Process-local ingress mailbox.  GES CONTROL dispatch is its sole producer;
 * the formation LMON tick is its sole consumer. */
extern ClusterReplacementPhase3IngressResult
cluster_replacement_wire_phase3_ingress_local(
	const ClusterICEnvelope *env, const void *payload, uint32 payload_length,
	int32 authenticated_source_node_id, int32 local_receiver_node_id,
	uint64 current_epoch, uint32 control_connection_generation);
extern bool cluster_replacement_phase3_handoff_poll_local(
	ClusterReplacementPhase3HandoffItem *out);
extern uint32 cluster_replacement_phase3_handoff_pending_local(void);
/* Raw owner observation, not authority: > capacity (including NULL) is
 * invalid, never empty. The caller must establish the owning LMON context. */
extern uint64
cluster_replacement_phase3_handoff_observed_count(const ClusterReplacementPhase3Handoff *handoff);
extern uint64 cluster_replacement_phase3_handoff_observed_count_local(void);


#endif /* CLUSTER_REPLACEMENT_WIRE_H */
