/*-------------------------------------------------------------------------
 *
 * cluster_replacement_wire.c
 *    Spec-5.15A opcode-18 replacement episode wire codec.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_replacement_wire.h"


#define REPLACEMENT_WIRE_OFF_OPCODE 0
#define REPLACEMENT_WIRE_OFF_PHASE 4
#define REPLACEMENT_WIRE_OFF_TARGET 8
#define REPLACEMENT_WIRE_OFF_VERSION 12
#define REPLACEMENT_WIRE_OFF_EPOCH 16
#define REPLACEMENT_WIRE_OFF_NONCE 24
#define REPLACEMENT_WIRE_OFF_IDENTITY0 32
#define REPLACEMENT_WIRE_OFF_IDENTITY1 40
#define REPLACEMENT_WIRE_OFF_BODY 48
#define REPLACEMENT_WIRE_OFF_FINGERPRINT 64


static ClusterReplacementPhase3Handoff replacement_phase3_local_handoff;


static void
replacement_wire_put_le32(uint8 *out, uint32 value)
{
	int i;

	for (i = 0; i < 4; i++)
		out[i] = (uint8)(value >> (i * 8));
}


static void
replacement_wire_put_le64(uint8 *out, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++)
		out[i] = (uint8)(value >> (i * 8));
}


static uint32
replacement_wire_get_le32(const uint8 *in)
{
	uint32 value = 0;
	int i;

	for (i = 0; i < 4; i++)
		value |= (uint32)in[i] << (i * 8);
	return value;
}


static uint64
replacement_wire_get_le64(const uint8 *in)
{
	uint64 value = 0;
	int i;

	for (i = 0; i < 8; i++)
		value |= (uint64)in[i] << (i * 8);
	return value;
}


static bool
replacement_wire_phase_valid(uint32 phase)
{
	return phase >= CLUSTER_REPLACEMENT_WIRE_PHASE_PURGE_REQUEST
		   && phase <= CLUSTER_REPLACEMENT_WIRE_PHASE_FAILSTOP_APPLICATION_ACK;
}


static bool
replacement_wire_message_valid(const ClusterReplacementWireMessage *message)
{
	if (message == NULL || !replacement_wire_phase_valid(message->phase))
		return false;
	if (message->grammar_fingerprint
		!= CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT)
		return false;
	if (message->phase
		== CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY)
		return message->body.phase3.jcmk_generation != 0
			   && message->body.phase3.episode_state_generation != 0
			   && message->body.phase3.reserved == 0;
	return true;
}


bool
cluster_replacement_wire_encode(const ClusterReplacementWireMessage *message,
								uint8 out[CLUSTER_REPLACEMENT_WIRE_BYTES])
{
	uint8 image[CLUSTER_REPLACEMENT_WIRE_BYTES];

	if (out == NULL || !replacement_wire_message_valid(message))
		return false;
	memset(image, 0, sizeof(image));
	replacement_wire_put_le32(image + REPLACEMENT_WIRE_OFF_OPCODE,
						  GES_REQ_OPCODE_REPLACEMENT_EPISODE);
	replacement_wire_put_le32(image + REPLACEMENT_WIRE_OFF_PHASE,
						  message->phase);
	replacement_wire_put_le32(image + REPLACEMENT_WIRE_OFF_TARGET,
						  (uint32)message->target_node_id);
	replacement_wire_put_le32(image + REPLACEMENT_WIRE_OFF_VERSION,
						  CLUSTER_REPLACEMENT_WIRE_VERSION);
	replacement_wire_put_le64(image + REPLACEMENT_WIRE_OFF_EPOCH,
						  message->epoch);
	replacement_wire_put_le64(image + REPLACEMENT_WIRE_OFF_NONCE,
						  message->request_nonce);
	replacement_wire_put_le64(image + REPLACEMENT_WIRE_OFF_IDENTITY0,
						  message->identity0);
	replacement_wire_put_le64(image + REPLACEMENT_WIRE_OFF_IDENTITY1,
						  message->identity1);
	if (message->phase
		== CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY) {
		replacement_wire_put_le64(image + REPLACEMENT_WIRE_OFF_BODY,
							  message->body.phase3.jcmk_generation);
		replacement_wire_put_le32(image + REPLACEMENT_WIRE_OFF_BODY + 8,
							  message->body.phase3.episode_state_generation);
	} else
		memcpy(image + REPLACEMENT_WIRE_OFF_BODY, message->body.bitmap,
			   CLUSTER_REPLACEMENT_WIRE_BITMAP_BYTES);
	replacement_wire_put_le64(image + REPLACEMENT_WIRE_OFF_FINGERPRINT,
						  message->grammar_fingerprint);
	memcpy(out, image, sizeof(image));
	return true;
}


/*
 * Convert the sole Startup-produced instantaneous READY snapshot into its
 * canonical phase-3 carrier.  This helper owns no READY state: it accepts only
 * an already-positive exact snapshot, writes through a private image, and
 * leaves the caller's output untouched on every refusal.
 */
bool
cluster_replacement_wire_encode_phase3_snapshot(
	const ClusterR4PrerequisiteSnapshot *snapshot,
	uint8 out[CLUSTER_REPLACEMENT_WIRE_BYTES])
{
	ClusterReplacementWireMessage message;
	uint8 image[CLUSTER_REPLACEMENT_WIRE_BYTES];

	if (snapshot == NULL || out == NULL
		|| snapshot->status != CLUSTER_R4_PREREQUISITE_R4A_READY
		|| !snapshot->ready
		|| snapshot->reserved0[0] != 0 || snapshot->reserved0[1] != 0
		|| snapshot->reserved0[2] != 0 || snapshot->target_node_id < 0
		|| snapshot->episode_state_generation == 0
		|| snapshot->jcmk_generation == 0 || snapshot->request_nonce == 0
		|| snapshot->old_admitted_incarnation == 0
		|| snapshot->fresh_incarnation <= snapshot->old_admitted_incarnation
		|| snapshot->committed_epoch == 0
		|| snapshot->grammar_fingerprint
			   != CANDIDATE2_CORRECTED_A1_GRAMMAR_FINGERPRINT)
		return false;

	memset(&message, 0, sizeof(message));
	message.phase = CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY;
	message.target_node_id = snapshot->target_node_id;
	message.epoch = snapshot->committed_epoch - 1;
	message.request_nonce = snapshot->request_nonce;
	message.identity0 = snapshot->old_admitted_incarnation;
	message.identity1 = snapshot->fresh_incarnation;
	message.body.phase3.jcmk_generation = snapshot->jcmk_generation;
	message.body.phase3.episode_state_generation
		= snapshot->episode_state_generation;
	message.grammar_fingerprint = snapshot->grammar_fingerprint;
	if (!cluster_replacement_wire_encode(&message, image))
		return false;
	memcpy(out, image, sizeof(image));
	return true;
}


bool
cluster_replacement_wire_decode(
	const uint8 bytes[CLUSTER_REPLACEMENT_WIRE_BYTES],
	ClusterReplacementWireMessage *out)
{
	ClusterReplacementWireMessage decoded;
	uint32 phase;

	if (bytes == NULL || out == NULL
		|| replacement_wire_get_le32(bytes + REPLACEMENT_WIRE_OFF_OPCODE)
			   != GES_REQ_OPCODE_REPLACEMENT_EPISODE
		|| replacement_wire_get_le32(bytes + REPLACEMENT_WIRE_OFF_VERSION)
			   != CLUSTER_REPLACEMENT_WIRE_VERSION)
		return false;
	phase = replacement_wire_get_le32(bytes + REPLACEMENT_WIRE_OFF_PHASE);
	if (!replacement_wire_phase_valid(phase))
		return false;
	if (phase == CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY
		&& replacement_wire_get_le32(bytes + REPLACEMENT_WIRE_OFF_BODY + 12)
			   != 0)
		return false;

	memset(&decoded, 0, sizeof(decoded));
	decoded.phase = phase;
	decoded.target_node_id
		= (int32)replacement_wire_get_le32(bytes + REPLACEMENT_WIRE_OFF_TARGET);
	decoded.epoch = replacement_wire_get_le64(bytes + REPLACEMENT_WIRE_OFF_EPOCH);
	decoded.request_nonce
		= replacement_wire_get_le64(bytes + REPLACEMENT_WIRE_OFF_NONCE);
	decoded.identity0
		= replacement_wire_get_le64(bytes + REPLACEMENT_WIRE_OFF_IDENTITY0);
	decoded.identity1
		= replacement_wire_get_le64(bytes + REPLACEMENT_WIRE_OFF_IDENTITY1);
	if (phase == CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY) {
		decoded.body.phase3.jcmk_generation
			= replacement_wire_get_le64(bytes + REPLACEMENT_WIRE_OFF_BODY);
		decoded.body.phase3.episode_state_generation
			= replacement_wire_get_le32(bytes + REPLACEMENT_WIRE_OFF_BODY + 8);
	} else
		memcpy(decoded.body.bitmap, bytes + REPLACEMENT_WIRE_OFF_BODY,
			   CLUSTER_REPLACEMENT_WIRE_BITMAP_BYTES);
	decoded.grammar_fingerprint
		= replacement_wire_get_le64(bytes + REPLACEMENT_WIRE_OFF_FINGERPRINT);
	if (!replacement_wire_message_valid(&decoded))
		return false;
	*out = decoded;
	return true;
}


void
cluster_replacement_phase3_handoff_init(ClusterReplacementPhase3Handoff *handoff)
{
	if (handoff != NULL)
		memset(handoff, 0, sizeof(*handoff));
}


uint32
cluster_replacement_phase3_handoff_pending(
	const ClusterReplacementPhase3Handoff *handoff)
{
	uint64 pending;

	if (handoff == NULL)
		return 0;
	pending = handoff->producer_seq - handoff->consumer_seq;
	if (pending > CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY)
		return 0;
	return (uint32)pending;
}


bool
cluster_replacement_phase3_handoff_poll(
	ClusterReplacementPhase3Handoff *handoff,
	ClusterReplacementPhase3HandoffItem *out)
{
	uint64 pending;

	if (handoff == NULL || out == NULL)
		return false;
	pending = handoff->producer_seq - handoff->consumer_seq;
	if (pending == 0 || pending > CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY)
		return false;
	*out = handoff->items[handoff->consumer_seq
						 % CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY];
	handoff->consumer_seq++;
	return true;
}


ClusterReplacementPhase3IngressResult
cluster_replacement_wire_phase3_ingress(
	ClusterReplacementPhase3Handoff *handoff, const ClusterICEnvelope *env,
	const void *payload, uint32 payload_length, int32 authenticated_source_node_id,
	int32 local_receiver_node_id, uint64 current_epoch,
	uint32 control_connection_generation)
{
	ClusterReplacementWireMessage message;
	ClusterReplacementPhase3HandoffItem item;
	uint64 pending;

	if (handoff == NULL || env == NULL || payload == NULL
		|| payload_length != CLUSTER_REPLACEMENT_WIRE_BYTES
		|| env->msg_type != PGRAC_IC_MSG_GES_REQUEST
		|| env->payload_length != CLUSTER_REPLACEMENT_WIRE_BYTES
		|| authenticated_source_node_id < 0 || local_receiver_node_id < 0
		|| authenticated_source_node_id == local_receiver_node_id
		|| env->source_node_id != (uint32)authenticated_source_node_id
		|| env->dest_node_id != (uint32)local_receiver_node_id
		|| env->epoch != current_epoch || control_connection_generation == 0)
		return CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED;

	if (!cluster_replacement_wire_decode((const uint8 *)payload, &message)
		|| message.phase
			   != CLUSTER_REPLACEMENT_WIRE_PHASE_TARGET_RECOVERY_READY
		|| message.target_node_id != authenticated_source_node_id
		|| message.epoch == UINT64_MAX || message.epoch + 1 != current_epoch
		|| message.request_nonce == 0
		|| message.identity0 == 0 || message.identity1 <= message.identity0)
		return CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED;

	pending = handoff->producer_seq - handoff->consumer_seq;
	if (pending >= CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY) {
		if (pending == CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY)
			return CLUSTER_REPLACEMENT_PHASE3_INGRESS_FULL;
		return CLUSTER_REPLACEMENT_PHASE3_INGRESS_REJECTED;
	}

	memset(&item, 0, sizeof(item));
	item.message = message;
	item.authenticated_source_node_id = authenticated_source_node_id;
	item.local_receiver_node_id = local_receiver_node_id;
	item.control_connection_generation = control_connection_generation;
	handoff->items[handoff->producer_seq
				   % CLUSTER_REPLACEMENT_PHASE3_HANDOFF_CAPACITY] = item;
	handoff->producer_seq++;
	return CLUSTER_REPLACEMENT_PHASE3_INGRESS_ENQUEUED;
}


ClusterReplacementPhase3IngressResult
cluster_replacement_wire_phase3_ingress_local(
	const ClusterICEnvelope *env, const void *payload, uint32 payload_length,
	int32 authenticated_source_node_id, int32 local_receiver_node_id,
	uint64 current_epoch, uint32 control_connection_generation)
{
	return cluster_replacement_wire_phase3_ingress(
		&replacement_phase3_local_handoff, env, payload, payload_length,
		authenticated_source_node_id, local_receiver_node_id, current_epoch,
		control_connection_generation);
}


bool
cluster_replacement_phase3_handoff_poll_local(
	ClusterReplacementPhase3HandoffItem *out)
{
	return cluster_replacement_phase3_handoff_poll(
		&replacement_phase3_local_handoff, out);
}


uint32
cluster_replacement_phase3_handoff_pending_local(void)
{
	return cluster_replacement_phase3_handoff_pending(
		&replacement_phase3_local_handoff);
}

uint64
cluster_replacement_phase3_handoff_observed_count(const ClusterReplacementPhase3Handoff *handoff)
{
	return handoff == NULL ? UINT64_MAX : handoff->producer_seq - handoff->consumer_seq;
}

uint64
cluster_replacement_phase3_handoff_observed_count_local(void)
{
	return cluster_replacement_phase3_handoff_observed_count(&replacement_phase3_local_handoff);
}
