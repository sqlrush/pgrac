/*-------------------------------------------------------------------------
 *
 * cluster_multixact_current_wire.c
 *	  Strict descriptor wire validation for current-DML MultiXact authority.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_multixact_current_wire.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.6b-multixact-current-dml.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_multixact_operation_wire.h"
#include "cluster/cluster_xid_stripe.h"
#include "storage/backendid.h"
#include "storage/off.h"


static bool
bytes_are_zero(const void *data, Size length)
{
	const uint8 *bytes = (const uint8 *)data;
	Size i;

	for (i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}


static bool
wire_epoch_valid(uint64 current_epoch)
{
	return current_epoch <= UINT32_MAX;
}


static bool
wire_mxkey_valid(const ClusterCurrentMxKey *key, uint64 current_epoch)
{
	return key != NULL && wire_epoch_valid(current_epoch) && key->origin_node_id < CLUSTER_MAX_NODES
		   && key->reserved16 == 0 && key->reserved32 == 0 && MultiXactIdIsValid(key->multixact_id)
		   && key->cluster_epoch == (uint32)current_epoch;
}


static bool
wire_mxkey_equal(const ClusterCurrentMxKey *a, const ClusterCurrentMxKey *b)
{
	return a != NULL && b != NULL && memcmp(a, b, sizeof(*a)) == 0;
}


static bool
wire_tt_key_valid(const ClusterTTStatusKey *key, uint64 current_epoch, int32 expected_origin,
				  TransactionId expected_xid)
{
	return key != NULL && wire_epoch_valid(current_epoch) && expected_origin >= 0
		   && expected_origin < CLUSTER_MAX_NODES && key->origin_node_id == (uint16)expected_origin
		   && key->undo_segment_id != 0 && key->tt_slot_id != 0
		   && key->cluster_epoch == (uint32)current_epoch && TransactionIdIsNormal(key->local_xid)
		   && (!TransactionIdIsValid(expected_xid) || key->local_xid == expected_xid)
		   && key->_reserved == 0 && key->_reserved2 == 0;
}


static bool
wire_proof_ask_valid(const ClusterCurrentMxProofAskWire *ask, uint32 total_count, int32 local_node)
{
	return ask != NULL && TransactionIdIsNormal(ask->xid) && ask->member_ordinal < total_count
		   && ask->member_status <= MaxMultiXactStatus && ask->reserved8 == 0
		   && cluster_xid_origin_slot(ask->xid) == local_node;
}


bool
cluster_multixact_current_wire_validate_describe_forward(const void *payload, uint32 payload_length,
														 int32 envelope_source, int32 local_node,
														 uint64 current_epoch,
														 ClusterCurrentMxDescribeForwardV2 *decoded)
{
	ClusterCurrentMxDescribeForwardV2 message;
	bool valid;

	if (decoded != NULL)
		memset(decoded, 0, sizeof(*decoded));
	if (payload == NULL || decoded == NULL || payload_length != sizeof(message)
		|| envelope_source < 0 || envelope_source >= CLUSTER_MAX_NODES || local_node < 0
		|| local_node >= CLUSTER_MAX_NODES || !wire_epoch_valid(current_epoch))
		return false;

	memcpy(&message, payload, sizeof(message));
	valid = message.prefix.request_id != 0 && message.prefix.epoch == current_epoch
			&& message.prefix.original_requester_node == envelope_source
			&& message.prefix.requester_backend_id > 0
			&& message.prefix.kind == GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE
			&& wire_mxkey_valid(&message.prefix.mxkey, current_epoch)
			&& message.prefix.mxkey.origin_node_id == (uint16)local_node
			&& bytes_are_zero(message.prefix.reserved_a, sizeof(message.prefix.reserved_a))
			&& bytes_are_zero(message.prefix.reserved_b, sizeof(message.prefix.reserved_b))
			&& message.trailer.magic == CLUSTER_CURRENT_MX_WIRE_MAGIC
			&& message.trailer.version == CLUSTER_CURRENT_MX_WIRE_VERSION
			&& message.trailer.flags == CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE
			&& bytes_are_zero(message.trailer.reserved, sizeof(message.trailer.reserved));
	if (!valid)
		return false;

	*decoded = message;
	return true;
}


bool
cluster_multixact_current_wire_validate_proof_forward(const void *payload, uint32 payload_length,
													  int32 envelope_source, int32 local_node,
													  uint64 current_epoch,
													  ClusterCurrentMxProofForwardV2 *decoded)
{
	ClusterCurrentMxProofForwardV2 message;
	uint16 semantic_chunk_count;
	uint8 i;

	if (decoded != NULL)
		memset(decoded, 0, sizeof(*decoded));
	if (payload == NULL || decoded == NULL || payload_length != sizeof(message)
		|| envelope_source < 0 || envelope_source >= CLUSTER_MAX_NODES || local_node < 0
		|| local_node >= CLUSTER_MAX_NODES || !wire_epoch_valid(current_epoch))
		return false;

	memcpy(&message, payload, sizeof(message));
	semantic_chunk_count = (uint16)message.prefix.chunk_count_minus_one + 1;
	if (message.prefix.request_id == 0 || message.prefix.epoch != current_epoch
		|| message.prefix.original_requester_node != envelope_source
		|| message.prefix.requester_backend_id <= 0
		|| message.prefix.kind != GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF
		|| !wire_mxkey_valid(&message.prefix.mxkey, current_epoch) || message.prefix.total_count < 1
		|| message.prefix.total_count > CLUSTER_CURRENT_MX_MAX_MEMBERS
		|| semantic_chunk_count > message.prefix.total_count
		|| semantic_chunk_count > CLUSTER_CURRENT_MX_MAX_CHUNKS
		|| message.prefix.chunk_ordinal >= semantic_chunk_count
		|| message.prefix.flags != CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE
		|| !bytes_are_zero(message.prefix.reserved_a, sizeof(message.prefix.reserved_a))
		|| !bytes_are_zero(message.prefix.reserved_b, sizeof(message.prefix.reserved_b))
		|| message.trailer.magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| message.trailer.version != CLUSTER_CURRENT_MX_WIRE_VERSION
		|| message.trailer.reserved16 != 0)
		return false;

	switch ((ClusterCurrentMxProofBodyKind)message.prefix.body_kind) {
	case CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS:
		if (message.prefix.entry_count == 0
			|| message.prefix.entry_count > CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME)
			return false;
		for (i = 0; i < message.prefix.entry_count; i++) {
			uint8 j;

			if (!wire_proof_ask_valid(&message.trailer.body.asks[i], message.prefix.total_count,
									  local_node))
				return false;
			for (j = 0; j < i; j++)
				if (message.trailer.body.asks[j].member_ordinal
						== message.trailer.body.asks[i].member_ordinal
					|| message.trailer.body.asks[j].xid == message.trailer.body.asks[i].xid)
					return false;
		}
		if (!bytes_are_zero(&message.trailer.body.asks[message.prefix.entry_count],
							sizeof(message.trailer.body)
								- message.prefix.entry_count
									  * sizeof(ClusterCurrentMxProofAskWire)))
			return false;
		break;

	case CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE: {
		const ClusterCurrentMxUpdaterChallengeWire *challenge
			= &message.trailer.body.updater.challenge;

		if (message.prefix.entry_count != 1 || !TransactionIdIsNormal(challenge->updater_xid)
			|| challenge->member_ordinal >= message.prefix.total_count
			|| challenge->member_status > MaxMultiXactStatus
			|| !ISUPDATE_from_mxstatus(challenge->member_status) || challenge->reserved8 != 0
			|| cluster_xid_origin_slot(challenge->updater_xid) != local_node
			|| !cluster_multixact_current_successor_provenance_well_formed(
				&challenge->candidate_next_xmin_alias, &challenge->candidate_next_xmin_locator,
				challenge->updater_xid, (uint16)local_node, (uint32)current_epoch))
			return false;
		break;
	}

	default:
		return false;
	}

	*decoded = message;
	return true;
}


static void
wire_init_proof_request(ClusterCurrentMxProofRequestPlan *plan, uint16 destination_node_id,
						const ClusterCurrentMxKey *key, uint64 descriptor_hash, uint32 total_count,
						uint64 request_id, uint64 current_epoch, int32 requester_node,
						int32 requester_backend_id, uint8 body_kind)
{
	memset(plan, 0, sizeof(*plan));
	plan->destination_node_id = destination_node_id;
	plan->request.prefix.request_id = request_id;
	plan->request.prefix.epoch = current_epoch;
	plan->request.prefix.mxkey = *key;
	plan->request.prefix.original_requester_node = requester_node;
	plan->request.prefix.requester_backend_id = requester_backend_id;
	plan->request.prefix.total_count = total_count;
	ClusterCurrentMxProofPrefixSetDescriptorHash(&plan->request.prefix, descriptor_hash);
	plan->request.prefix.body_kind = body_kind;
	plan->request.prefix.kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	plan->request.trailer.magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	plan->request.trailer.version = CLUSTER_CURRENT_MX_WIRE_VERSION;
}


ClusterMxResolveResult
cluster_multixact_current_wire_build_proof_requests(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	const uint16 *member_origin_nodes, uint16 nmembers, uint64 descriptor_hash,
	const ClusterCurrentUpdaterChallenge *challenge, uint64 request_id, uint64 current_epoch,
	int32 requester_node, int32 requester_backend_id, ClusterCurrentMxProofRequestPlan *plans,
	uint16 plans_cap, uint16 *plan_count)
{
	uint16 origin_counts[CLUSTER_MAX_NODES];
	int updater_ordinal = -1;
	uint16 required_plans = 0;
	uint16 built = 0;
	uint16 origin;
	uint16 i;
	/* The auxiliary undo cleaner owns CTRC progress and has no BackendId.
	 * It may consume a same-node proof plan directly; such a plan remains
	 * invalid to every wire validator and may never target a foreign node. */
	bool local_only = requester_backend_id == InvalidBackendId;

	if (plan_count != NULL)
		*plan_count = 0;
	if (plans != NULL && plans_cap > 0)
		memset(plans, 0, sizeof(*plans) * plans_cap);
	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RESOLVE_SUPPORTED_LIMIT;
	if (key == NULL || members == NULL || member_origin_nodes == NULL || plans == NULL
		|| plan_count == NULL || nmembers < 1 || request_id == 0 || !wire_epoch_valid(current_epoch)
		|| requester_node < 0 || requester_node >= CLUSTER_MAX_NODES
		|| (requester_backend_id <= 0 && !local_only) || !wire_mxkey_valid(key, current_epoch)
		|| cluster_multixact_current_validate_descriptor(
			   key, key->origin_node_id, (uint32)current_epoch, members, nmembers, nmembers)
			   != CMX_DESC_OK
		|| descriptor_hash != cluster_multixact_current_descriptor_hash(key, members, nmembers))
		return CMX_RESOLVE_UNKNOWN;

	memset(origin_counts, 0, sizeof(origin_counts));
	for (i = 0; i < nmembers; i++) {
		int derived_origin = cluster_xid_origin_slot(members[i].xid);

		if (derived_origin < 0 || derived_origin >= CLUSTER_MAX_NODES
			|| member_origin_nodes[i] != (uint16)derived_origin
			|| (local_only && derived_origin != requester_node))
			return CMX_RESOLVE_UNKNOWN;
		if (ISUPDATE_from_mxstatus(members[i].member_status))
			updater_ordinal = i;
		origin_counts[derived_origin]++;
	}

	if (challenge != NULL) {
		if (updater_ordinal < 0 || challenge->reserved16 != 0
			|| challenge->member_ordinal != (uint16)updater_ordinal
			|| challenge->updater_xid != members[updater_ordinal].xid
			|| !cluster_multixact_current_successor_provenance_well_formed(
				&challenge->candidate_next_xmin_alias, &challenge->candidate_next_xmin_locator,
				members[updater_ordinal].xid, member_origin_nodes[updater_ordinal],
				(uint32)current_epoch))
			return CMX_RESOLVE_UNKNOWN;
		Assert(origin_counts[member_origin_nodes[updater_ordinal]] > 0);
		origin_counts[member_origin_nodes[updater_ordinal]]--;
		required_plans++;
	}

	for (origin = 0; origin < CLUSTER_MAX_NODES; origin++)
		if (origin_counts[origin] > 0)
			required_plans
				+= (origin_counts[origin] + CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME - 1)
				   / CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME;
	if (required_plans == 0 || required_plans > CLUSTER_CURRENT_MX_MAX_CHUNKS
		|| required_plans > plans_cap)
		return CMX_RESOLVE_UNKNOWN;

	for (origin = 0; origin < CLUSTER_MAX_NODES; origin++) {
		ClusterCurrentMxProofRequestPlan *plan = NULL;

		for (i = 0; i < nmembers; i++) {
			ClusterCurrentMxProofAskWire *ask;

			if (member_origin_nodes[i] != origin
				|| (challenge != NULL && i == (uint16)updater_ordinal))
				continue;
			if (plan == NULL
				|| plan->request.prefix.entry_count
					   == CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME) {
				plan = &plans[built++];
				wire_init_proof_request(plan, origin, key, descriptor_hash, nmembers, request_id,
										current_epoch, requester_node, requester_backend_id,
										CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS);
			}
			ask = &plan->request.trailer.body.asks[plan->request.prefix.entry_count++];
			ask->xid = members[i].xid;
			ask->member_ordinal = i;
			ask->member_status = members[i].member_status;
		}
	}

	if (challenge != NULL) {
		ClusterCurrentMxProofRequestPlan *plan = &plans[built++];
		ClusterCurrentMxUpdaterChallengeWire *wire_challenge;

		wire_init_proof_request(plan, member_origin_nodes[updater_ordinal], key, descriptor_hash,
								nmembers, request_id, current_epoch, requester_node,
								requester_backend_id,
								CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE);
		plan->request.prefix.entry_count = 1;
		wire_challenge = &plan->request.trailer.body.updater.challenge;
		wire_challenge->candidate_next_xmin_alias = challenge->candidate_next_xmin_alias;
		wire_challenge->candidate_next_xmin_locator = challenge->candidate_next_xmin_locator;
		wire_challenge->updater_xid = challenge->updater_xid;
		wire_challenge->member_ordinal = challenge->member_ordinal;
		wire_challenge->member_status = members[updater_ordinal].member_status;
	}

	Assert(built == required_plans);
	for (i = 0; i < built; i++) {
		plans[i].request.prefix.chunk_ordinal = (uint8)i;
		plans[i].request.prefix.chunk_count_minus_one = (uint8)(built - 1);
	}
	*plan_count = built;
	return CMX_RESOLVE_OK;
}


static void
wire_proof_outputs_reset(ClusterCurrentMemberProof *proofs, uint16 proofs_cap, uint16 *proof_count,
						 ClusterCurrentUpdaterProof *updater_proof,
						 uint32 *requester_capability_generation_out)
{
	uint16 i;

	if (proofs != NULL) {
		memset(proofs, 0, sizeof(*proofs) * proofs_cap);
		for (i = 0; i < proofs_cap; i++)
			proofs[i].state = CCM_UNKNOWN;
	}
	if (proof_count != NULL)
		*proof_count = 0;
	if (updater_proof != NULL) {
		memset(updater_proof, 0, sizeof(*updater_proof));
		updater_proof->verdict = CUCP_UNKNOWN;
	}
	if (requester_capability_generation_out != NULL)
		*requester_capability_generation_out = 0;
}


static bool
wire_member_proof_valid(const ClusterCurrentMemberProof *proof,
						const ClusterCurrentMxProofAskWire *ask, int32 expected_source,
						uint64 current_epoch)
{
	static const ClusterCurrentMemberProofKey zero_key;
	ClusterTTStatusKey status_key;
	uint32 segment_generation;
	uint16 slot_wrap;

	if (proof == NULL || ask == NULL || proof->member_ordinal != ask->member_ordinal
		|| proof->member_xid != ask->xid || proof->member_status != ask->member_status
		|| proof->state > CCM_UNKNOWN)
		return false;

	switch ((ClusterCurrentMemberState)proof->state) {
	case CCM_ACTIVE:
		return ClusterCurrentMemberProofGetCtrcGrant(proof) != 0 && proof->commit_scn == InvalidScn
			   && ClusterCurrentMemberProofGetStatusKey(proof, &status_key, &segment_generation,
														&slot_wrap)
			   && wire_tt_key_valid(&status_key, current_epoch, expected_source,
									InvalidTransactionId)
			   && (status_key.local_xid == ask->xid
				   || TransactionIdPrecedes(status_key.local_xid, ask->xid));
	case CCM_COMMITTED:
		return ClusterCurrentMemberProofGetCtrcGrant(proof) == 0
			   && memcmp(&proof->key, &zero_key, sizeof(zero_key)) == 0
			   && SCN_VALID(proof->commit_scn);
	case CCM_ABORTED:
		return ClusterCurrentMemberProofGetCtrcGrant(proof) == 0
			   && memcmp(&proof->key, &zero_key, sizeof(zero_key)) == 0
			   && proof->commit_scn == InvalidScn;
	case CCM_SELF:
	case CCM_UNKNOWN:
		return false;
	}

	return false;
}


static bool
wire_updater_proof_valid(const ClusterCurrentUpdaterProof *proof,
						 const ClusterCurrentMxProofForwardV2 *request)
{
	const ClusterCurrentMxUpdaterChallengeWire *challenge
		= &request->trailer.body.updater.challenge;

	return proof != NULL && wire_mxkey_equal(&proof->mxkey, &request->prefix.mxkey)
		   && memcmp(&proof->candidate_next_xmin_alias, &challenge->candidate_next_xmin_alias,
					 sizeof(ClusterCurrentMxSuccessorAlias))
				  == 0
		   && proof->candidate_next_xmin_locator.tt_wrap <= TT_WRAP_MAX
		   && cluster_tx_locator_reply_matches(&challenge->candidate_next_xmin_locator,
											   &proof->candidate_next_xmin_locator)
		   && proof->updater_xid == challenge->updater_xid
		   && proof->member_ordinal == challenge->member_ordinal && proof->verdict < CUCP_UNKNOWN
		   && proof->reserved8 == 0;
}


ClusterMxResolveResult
cluster_multixact_current_wire_validate_proof_reply(
	const void *payload, uint32 payload_length, int32 expected_source, uint64 current_epoch,
	const ClusterCurrentMxProofForwardV2 *expected_request, ClusterCurrentMemberProof *proofs,
	uint16 proofs_cap, uint16 *proof_count, ClusterCurrentUpdaterProof *updater_proof,
	uint32 *requester_capability_generation_out)
{
	ClusterCurrentMxProofForwardV2 decoded_request;
	ClusterCurrentMxProofReplyPage page;
	const ClusterCurrentMxProofReplyHeader *header;
	ClusterMxResolveResult result;
	Size expected_wire_length;
	uint8 i;

	wire_proof_outputs_reset(proofs, proofs_cap, proof_count, updater_proof,
							 requester_capability_generation_out);
	if (payload == NULL || payload_length != sizeof(page) || expected_source < 0
		|| expected_source >= CLUSTER_MAX_NODES || expected_request == NULL || proof_count == NULL
		|| updater_proof == NULL
		|| !cluster_multixact_current_wire_validate_proof_forward(
			expected_request, sizeof(*expected_request),
			expected_request->prefix.original_requester_node, expected_source, current_epoch,
			&decoded_request))
		return CMX_RESOLVE_UNKNOWN;

	memcpy(&page, payload, sizeof(page));
	header = &page.header;
	if (header->magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| header->version != CLUSTER_CURRENT_MX_WIRE_VERSION
		|| header->kind != GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF
		|| header->flags != CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE
		|| header->source_node_id != (uint32)expected_source
		|| header->request_id != decoded_request.prefix.request_id
		|| !wire_mxkey_equal(&header->mxkey, &decoded_request.prefix.mxkey)
		|| header->descriptor_hash
			   != ClusterCurrentMxProofPrefixGetDescriptorHash(&decoded_request.prefix)
		|| header->total_count != decoded_request.prefix.total_count
		|| header->chunk_ordinal != decoded_request.prefix.chunk_ordinal
		|| header->chunk_count_minus_one != decoded_request.prefix.chunk_count_minus_one
		|| header->result > CMX_RESOLVE_RETRY || header->result == CMX_RESOLVE_TIMEOUT
		|| header->reserved16 != 0 || header->wire_length < sizeof(*header)
		|| header->wire_length > sizeof(page))
		return CMX_RESOLVE_UNKNOWN;

	result = (ClusterMxResolveResult)header->result;
	if (result != CMX_RESOLVE_OK) {
		if (header->requester_capability_generation != 0 || header->entry_count != 0
			|| header->wire_length != sizeof(*header)
			|| !bytes_are_zero((const uint8 *)&page + sizeof(*header),
							   sizeof(page) - sizeof(*header)))
			return CMX_RESOLVE_UNKNOWN;
		return result;
	}
	if (header->requester_capability_generation == 0 || requester_capability_generation_out == NULL)
		return CMX_RESOLVE_UNKNOWN;

	if (proofs == NULL)
		return CMX_RESOLVE_UNKNOWN;
	switch ((ClusterCurrentMxProofBodyKind)decoded_request.prefix.body_kind) {
	case CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS:
		expected_wire_length
			= sizeof(*header)
			  + decoded_request.prefix.entry_count * sizeof(ClusterCurrentMemberProof);
		if (header->entry_count != decoded_request.prefix.entry_count
			|| header->entry_count > proofs_cap || header->wire_length != expected_wire_length)
			return CMX_RESOLVE_UNKNOWN;
		for (i = 0; i < header->entry_count; i++)
			if (!wire_member_proof_valid(&page.body.proofs[i],
										 &decoded_request.trailer.body.asks[i], expected_source,
										 current_epoch))
				return CMX_RESOLVE_UNKNOWN;
		memcpy(proofs, page.body.proofs, sizeof(*proofs) * header->entry_count);
		break;

	case CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE: {
		ClusterCurrentMxProofAskWire updater_ask;

		memset(&updater_ask, 0, sizeof(updater_ask));
		updater_ask.xid = decoded_request.trailer.body.updater.challenge.updater_xid;
		updater_ask.member_ordinal = decoded_request.trailer.body.updater.challenge.member_ordinal;
		updater_ask.member_status = decoded_request.trailer.body.updater.challenge.member_status;
		expected_wire_length = sizeof(*header) + sizeof(ClusterCurrentMemberProof)
							   + sizeof(ClusterCurrentUpdaterProof);
		if (header->entry_count != 1 || proofs_cap < 1
			|| header->wire_length != expected_wire_length
			|| !wire_member_proof_valid(&page.body.updater.member_proof, &updater_ask,
										expected_source, current_epoch)
			|| !wire_updater_proof_valid(&page.body.updater.updater_proof, &decoded_request))
			return CMX_RESOLVE_UNKNOWN;
		proofs[0] = page.body.updater.member_proof;
		*updater_proof = page.body.updater.updater_proof;
		break;
	}

	default:
		return CMX_RESOLVE_UNKNOWN;
	}

	if (!bytes_are_zero((const uint8 *)&page + header->wire_length,
						sizeof(page) - header->wire_length)) {
		wire_proof_outputs_reset(proofs, proofs_cap, proof_count, updater_proof,
								 requester_capability_generation_out);
		return CMX_RESOLVE_UNKNOWN;
	}

	*proof_count = header->entry_count;
	*requester_capability_generation_out = header->requester_capability_generation;
	return CMX_RESOLVE_OK;
}


bool
cluster_multixact_current_wire_validate_proof_reply_frame(
	const void *payload, uint32 payload_length, int32 expected_source, uint64 current_epoch,
	const ClusterCurrentMxProofForwardV2 *expected_request, ClusterMxResolveResult *result,
	ClusterCurrentMemberProof *proofs, uint16 proofs_cap, uint16 *proof_count,
	ClusterCurrentUpdaterProof *updater_proof, uint32 *requester_capability_generation_out)
{
	ClusterMxResolveResult decoded_result;

	if (result != NULL)
		*result = CMX_RESOLVE_UNKNOWN;
	if (requester_capability_generation_out != NULL)
		*requester_capability_generation_out = 0;
	if (result == NULL)
		return false;
	decoded_result = cluster_multixact_current_wire_validate_proof_reply(
		payload, payload_length, expected_source, current_epoch, expected_request, proofs,
		proofs_cap, proof_count, updater_proof, requester_capability_generation_out);
	if (decoded_result != CMX_RESOLVE_UNKNOWN) {
		*result = decoded_result;
		return true;
	}

	if (payload != NULL && payload_length == sizeof(ClusterCurrentMxProofReplyPage)) {
		ClusterCurrentMxProofReplyPage page;
		ClusterCurrentMemberProof scratch_proofs[CLUSTER_CURRENT_MX_MAX_PROOF_ASKS_PER_FRAME];
		ClusterCurrentUpdaterProof scratch_updater;
		uint16 scratch_count = 0;
		uint32 scratch_capability_generation = 0;

		memcpy(&page, payload, sizeof(page));
		if (page.header.result == CMX_RESOLVE_UNKNOWN) {
			page.header.result = CMX_RESOLVE_DENIED;
			if (cluster_multixact_current_wire_validate_proof_reply(
					&page, sizeof(page), expected_source, current_epoch, expected_request,
					scratch_proofs, lengthof(scratch_proofs), &scratch_count, &scratch_updater,
					&scratch_capability_generation)
				== CMX_RESOLVE_DENIED) {
				*result = CMX_RESOLVE_UNKNOWN;
				return true;
			}
		}
	}
	return false;
}


ClusterMxDescribeResult
cluster_multixact_current_wire_validate_describe_reply(const void *payload, uint32 payload_length,
													   int32 expected_source, uint64 current_epoch,
													   uint64 expected_request_id,
													   const ClusterCurrentMxKey *expected_key,
													   ClusterCurrentMxMemberDesc *members,
													   uint16 members_cap, uint16 *members_count,
													   uint32 *reported_total_members)
{
	ClusterCurrentMxDescribeReplyPage page;
	const ClusterCurrentMxDescribeReplyHeader *header;
	ClusterMxDescribeResult result;
	Size expected_wire_length;

	if (members != NULL && members_cap > 0)
		memset(members, 0, sizeof(*members) * members_cap);
	if (members_count != NULL)
		*members_count = 0;
	if (reported_total_members != NULL)
		*reported_total_members = 0;

	if (payload == NULL || payload_length != sizeof(page) || expected_source < 0
		|| expected_source >= CLUSTER_MAX_NODES || expected_request_id == 0
		|| !wire_mxkey_valid(expected_key, current_epoch)
		|| expected_key->origin_node_id != (uint16)expected_source)
		return CMX_DESC_UNKNOWN;

	memcpy(&page, payload, sizeof(page));
	header = &page.header;
	if (header->magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| header->version != CLUSTER_CURRENT_MX_WIRE_VERSION
		|| header->kind != GCS_BLOCK_FORWARD_KIND_CURRENT_MX_DESCRIBE
		|| header->flags != CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE
		|| header->source_node_id != (uint32)expected_source
		|| header->request_id != expected_request_id
		|| !wire_mxkey_equal(&header->mxkey, expected_key) || header->result > CMX_DESC_UNKNOWN
		|| header->result == CMX_DESC_TIMEOUT || header->chunk_ordinal != 0
		|| header->chunk_count_minus_one != 0 || header->reserved16 != 0 || header->reserved32 != 0
		|| header->wire_length < sizeof(*header) || header->wire_length > sizeof(page))
		return CMX_DESC_UNKNOWN;

	result = (ClusterMxDescribeResult)header->result;
	if (result != CMX_DESC_OK) {
		if (header->entry_count != 0 || header->descriptor_hash != 0
			|| header->wire_length != sizeof(*header)
			|| !bytes_are_zero((const uint8 *)&page + sizeof(*header),
							   sizeof(page) - sizeof(*header)))
			return CMX_DESC_UNKNOWN;
		if (result == CMX_DESC_SUPPORTED_LIMIT) {
			if (header->total_count <= CLUSTER_CURRENT_MX_MAX_MEMBERS)
				return CMX_DESC_UNKNOWN;
			if (reported_total_members != NULL)
				*reported_total_members = header->total_count;
		} else if (header->total_count != 0)
			return CMX_DESC_UNKNOWN;
		return result;
	}

	expected_wire_length = sizeof(*header) + sizeof(*page.members) * (Size)header->entry_count;
	if (header->total_count != header->entry_count || header->entry_count > members_cap
		|| header->entry_count > CLUSTER_CURRENT_MX_MAX_MEMBERS
		|| header->wire_length != expected_wire_length || members == NULL || members_count == NULL
		|| reported_total_members == NULL
		|| cluster_multixact_current_validate_descriptor(expected_key, (uint16)expected_source,
														 (uint32)current_epoch, page.members,
														 header->entry_count, header->total_count)
			   != CMX_DESC_OK
		|| header->descriptor_hash
			   != cluster_multixact_current_descriptor_hash(expected_key, page.members,
															header->entry_count)
		|| !bytes_are_zero((const uint8 *)&page + header->wire_length,
						   sizeof(page) - header->wire_length))
		return CMX_DESC_UNKNOWN;

	memcpy(members, page.members, sizeof(*members) * header->entry_count);
	*members_count = header->entry_count;
	*reported_total_members = header->total_count;
	return CMX_DESC_OK;
}


/* Dormant format-6 codecs. No caller may treat structural validation as
 * original TT/DATA qualification, retention, admission, or a CTRC grant. */

/* Native-endian fields are copied individually, including on unaligned input.
 * Never copy the four padding bytes at the end of the old 48-byte proof. */
#define OP_WIRE_FIELD(bytes, offset, field, encode)                                                \
	do {                                                                                           \
		if (encode)                                                                                \
			memcpy((bytes) + (offset), &(field), sizeof(field));                                   \
		else                                                                                       \
			memcpy(&(field), (bytes) + (offset), sizeof(field));                                   \
	} while (0)

static void
operation_wire_locator(uint8 *bytes, ClusterTxLocator *locator, bool encode)
{
	OP_WIRE_FIELD(bytes, 0, locator->uba.raw[0], encode);
	OP_WIRE_FIELD(bytes, 8, locator->uba.raw[1], encode);
	OP_WIRE_FIELD(bytes, 16, locator->xid, encode);
	OP_WIRE_FIELD(bytes, 20, locator->tt_wrap, encode);
	OP_WIRE_FIELD(bytes, 22, locator->itl_kind, encode);
	OP_WIRE_FIELD(bytes, 23, locator->itl_slot_index, encode);
}

static bool
operation_wire_key_valid(const ClusterSavepointOperationKey *key, TransactionId member,
						 int32 origin)
{
	uint32 segment, block;
	uint16 slot, row;
	return key != NULL && TransactionIdIsNormal(member) && TransactionIdIsNormal(key->locator.xid)
		   && XidFromFullTransactionId(key->top_fxid) == key->locator.xid
		   && XidFromFullTransactionId(key->operation_fxid) == member
		   && U64FromFullTransactionId(key->top_fxid) != UINT64_MAX
		   && U64FromFullTransactionId(key->operation_fxid) != UINT64_MAX
		   && FullTransactionIdFollows(key->operation_fxid, key->top_fxid)
		   && key->locator.tt_wrap <= TT_WRAP_MAX
		   && key->locator.itl_slot_index < CLUSTER_ITL_INITRANS_DEFAULT
		   && ((key->locator.itl_kind >= ITL_FLAG_ACTIVE
				&& key->locator.itl_kind <= ITL_FLAG_NEEDS_CLEANOUT)
			   || ITL_FLAG_IS_LOCK_ONLY(key->locator.itl_kind))
		   && uba_decode_record(key->locator.uba, &segment, &block, &slot, &row)
		   && (segment - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE == origin
		   && cluster_xid_origin_slot(key->locator.xid) == origin && key->target._pad == 0
		   && RelFileNumberIsValid(key->target.locator.relNumber)
		   && key->target.forknum == MAIN_FORKNUM && key->target.blockno != InvalidBlockNumber
		   && OffsetNumberIsValid(key->target.offnum);
}

static void
operation_wire_key_fields(uint8 *bytes, ClusterSavepointOperationKey *key, bool encode)
{
	operation_wire_locator(bytes + 8, &key->locator, encode);
	OP_WIRE_FIELD(bytes, 48, key->target.locator.spcOid, encode);
	OP_WIRE_FIELD(bytes, 52, key->target.locator.dbOid, encode);
	OP_WIRE_FIELD(bytes, 56, key->target.locator.relNumber, encode);
	OP_WIRE_FIELD(bytes, 60, key->target.forknum, encode);
	OP_WIRE_FIELD(bytes, 64, key->target.blockno, encode);
	OP_WIRE_FIELD(bytes, 68, key->target.offnum, encode);
	OP_WIRE_FIELD(bytes, 70, key->target._pad, encode);
	OP_WIRE_FIELD(bytes, 72, key->top_fxid, encode);
	OP_WIRE_FIELD(bytes, 80, key->operation_fxid, encode);
}

static bool
operation_wire_read_ask(const ClusterCurrentMxOperationForwardV6 *request, uint16 index,
						int32 origin, ClusterCurrentMxOperationAsk *out)
{
	ClusterCurrentMxOperationAsk ask = { 0 };
	uint8 bytes[96];
	uint16 version, length;
	Size offset;
	if (out == NULL || request == NULL || index >= request->prefix.entry_count || index >= 7)
		return false;
	offset
		= request->prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS ? index * 96 : 56;
	memcpy(bytes, request->body + offset, sizeof(bytes));
	memcpy(&ask.member, bytes, sizeof(ask.member));
	ask.member.reserved8 = 0; /* byte 7 belongs to the new explicit flags field */
	if (!wire_proof_ask_valid(&ask.member, request->prefix.total_count, origin))
		return false;
	if (bytes[7] == 0) {
		if (!bytes_are_zero(bytes + 8, 88))
			return false;
	} else if (bytes[7] == 1) {
		ClusterUndoByteAddress uba;
		operation_wire_key_fields(bytes, &ask.operation, false);
		memcpy(&uba, bytes + 32, sizeof(uba));
		memcpy(&version, bytes + 88, sizeof(version));
		memcpy(&length, bytes + 90, sizeof(length));
		if (version != 1 || length != 96 || !bytes_are_zero(bytes + 92, 4)
			|| memcmp(&uba, &ask.operation.locator.uba, sizeof(uba)) != 0
			|| !operation_wire_key_valid(&ask.operation, ask.member.xid, origin))
			return false;
		ask.exact = true;
	} else
		return false;
	*out = ask;
	return true;
}

static void
operation_wire_write_ask(uint8 bytes[96], const ClusterCurrentMxProofAskWire *member,
						 const ClusterSavepointOperationKey *operation)
{
	memset(bytes, 0, 96);
	memcpy(bytes, member, sizeof(*member));
	if (operation != NULL && !bytes_are_zero(operation, sizeof(*operation))) {
		ClusterSavepointOperationKey key = *operation;
		uint16 version = 1, length = 96;
		bytes[7] = 1;
		operation_wire_key_fields(bytes, &key, true);
		memcpy(bytes + 32, &key.locator.uba, sizeof(key.locator.uba));
		memcpy(bytes + 88, &version, sizeof(version));
		memcpy(bytes + 90, &length, sizeof(length));
	}
}

ClusterMxResolveResult
cluster_multixact_operation_wire_build_requests(
	const ClusterCurrentMxKey *key, const ClusterCurrentMxMemberDesc *members,
	const uint16 *origins, const ClusterSavepointOperationKey *operations, uint16 nmembers,
	uint64 descriptor_hash, const ClusterCurrentUpdaterChallenge *challenge, uint64 request_id,
	uint64 epoch, int32 requester, int32 backend, ClusterCurrentMxOperationRequestPlan *plans,
	uint16 capacity, uint16 *count)
{
	/* Reuse descriptor validation and the exact existing grouping algorithm.
  * These are semantic scratch plans, never sent as a fallback wire frame. */
	ClusterCurrentMxProofRequestPlan grouped[CLUSTER_CURRENT_MX_MAX_CHUNKS];
	ClusterMxResolveResult result;
	uint16 n = 0;
	if (count != NULL)
		*count = 0;
	if (plans != NULL)
		memset(plans, 0, sizeof(*plans) * capacity);
	if (nmembers > CLUSTER_CURRENT_MX_MAX_MEMBERS)
		return CMX_RESOLVE_SUPPORTED_LIMIT;
	if (plans == NULL || count == NULL || backend <= 0)
		return CMX_RESOLVE_UNKNOWN;
	result = cluster_multixact_current_wire_build_proof_requests(
		key, members, origins, nmembers, descriptor_hash, challenge, request_id, epoch, requester,
		backend, grouped, Min(capacity, CLUSTER_CURRENT_MX_MAX_CHUNKS), &n);
	if (result != CMX_RESOLVE_OK)
		return result;
	for (uint16 i = 0; operations != NULL && i < nmembers; i++)
		if (!bytes_are_zero(&operations[i], sizeof(operations[i]))
			&& !operation_wire_key_valid(&operations[i], members[i].xid, origins[i]))
			return CMX_RESOLVE_UNKNOWN;
	for (uint16 i = 0; i < n; i++) {
		const ClusterCurrentMxProofForwardV2 *old = &grouped[i].request;
		ClusterCurrentMxOperationForwardV6 *next = &plans[i].request;
		plans[i].destination_node_id = grouped[i].destination_node_id;
		next->prefix = old->prefix;
		next->magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
		next->version = CLUSTER_CURRENT_MX_OPERATION_WIRE_VERSION;
		for (uint16 j = 0; j < old->prefix.entry_count; j++) {
			ClusterCurrentMxProofAskWire ask = { 0 };
			Size offset;
			if (old->prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
				const ClusterCurrentMxUpdaterChallengeWire *up
					= &old->trailer.body.updater.challenge;
				memcpy(next->body, up, sizeof(*up));
				ask.xid = up->updater_xid;
				ask.member_ordinal = up->member_ordinal;
				ask.member_status = up->member_status;
				offset = 56;
			} else {
				ask = old->trailer.body.asks[j];
				offset = j * 96;
			}
			operation_wire_write_ask(next->body + offset, &ask,
									 operations == NULL ? NULL : &operations[ask.member_ordinal]);
		}
	}
	*count = n;
	return CMX_RESOLVE_OK;
}

bool
cluster_multixact_operation_wire_validate_forward(const void *payload, uint32 length, int32 source,
												  int32 local, uint64 epoch,
												  ClusterCurrentMxOperationForwardV6 *out)
{
	ClusterCurrentMxOperationForwardV6 request;
	ClusterCurrentMxOperationAsk asks[7];
	uint16 chunks;
	Size used;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (payload == NULL || out == NULL || length != sizeof(request) || source < 0
		|| source >= CLUSTER_MAX_NODES || local < 0 || local >= CLUSTER_MAX_NODES
		|| !wire_epoch_valid(epoch))
		return false;
	memcpy(&request, payload, sizeof(request));
	chunks = (uint16)request.prefix.chunk_count_minus_one + 1;
	if (request.magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| request.version != CLUSTER_CURRENT_MX_OPERATION_WIRE_VERSION || request.reserved16 != 0
		|| request.prefix.request_id == 0 || request.prefix.epoch != epoch
		|| request.prefix.original_requester_node != source
		|| request.prefix.requester_backend_id <= 0
		|| request.prefix.kind != GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF
		|| !wire_mxkey_valid(&request.prefix.mxkey, epoch) || request.prefix.total_count < 1
		|| request.prefix.total_count > CLUSTER_CURRENT_MX_MAX_MEMBERS
		|| chunks > request.prefix.total_count || chunks > CLUSTER_CURRENT_MX_MAX_CHUNKS
		|| request.prefix.chunk_ordinal >= chunks || request.prefix.flags != 0
		|| request.prefix.entry_count == 0 || request.prefix.entry_count > 7
		|| !bytes_are_zero(request.prefix.reserved_a, sizeof(request.prefix.reserved_a))
		|| !bytes_are_zero(request.prefix.reserved_b, sizeof(request.prefix.reserved_b))
		|| !bytes_are_zero(request.reserved, sizeof(request.reserved)))
		return false;
	if (request.prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
		ClusterCurrentMxUpdaterChallengeWire up;
		memcpy(&up, request.body, sizeof(up));
		if (request.prefix.entry_count != 1 || up.reserved8 != 0
			|| !ISUPDATE_from_mxstatus(up.member_status)
			|| !cluster_multixact_current_successor_provenance_well_formed(
				&up.candidate_next_xmin_alias, &up.candidate_next_xmin_locator, up.updater_xid,
				local, (uint32)epoch)
			|| !operation_wire_read_ask(&request, 0, local, &asks[0])
			|| up.updater_xid != asks[0].member.xid
			|| up.member_status != asks[0].member.member_status
			|| up.member_ordinal != asks[0].member.member_ordinal)
			return false;
		used = 152;
	} else if (request.prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS) {
		for (uint16 i = 0; i < request.prefix.entry_count; i++) {
			if (!operation_wire_read_ask(&request, i, local, &asks[i]))
				return false;
			for (uint16 j = 0; j < i; j++)
				if (asks[j].member.xid == asks[i].member.xid
					|| asks[j].member.member_ordinal == asks[i].member.member_ordinal)
					return false;
		}
		used = request.prefix.entry_count * 96;
	} else
		return false;
	if (!bytes_are_zero(request.body + used, sizeof(request.body) - used))
		return false;
	*out = request;
	return true;
}

bool
cluster_multixact_operation_wire_get_ask(const ClusterCurrentMxOperationForwardV6 *request,
										 uint16 index, ClusterCurrentMxOperationAsk *out)
{
	ClusterCurrentMxOperationForwardV6 checked;
	TransactionId xid;
	Size offset;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || request == NULL || index >= request->prefix.entry_count || index >= 7)
		return false;
	offset
		= request->prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_MEMBER_ASKS ? index * 96 : 56;
	memcpy(&xid, request->body + offset, sizeof(xid));
	if (!cluster_multixact_operation_wire_validate_forward(
			request, sizeof(*request), request->prefix.original_requester_node,
			cluster_xid_origin_slot(xid), request->prefix.epoch, &checked))
		return false;
	return operation_wire_read_ask(&checked, index, cluster_xid_origin_slot(xid), out);
}

static void
operation_wire_proof_fields(uint8 *bytes, ClusterCurrentMemberOperationProof *p, bool encode)
{
	OP_WIRE_FIELD(bytes, 0, p->base.key.origin_node_id, encode);
	OP_WIRE_FIELD(bytes, 2, p->base.key.undo_segment_id, encode);
	OP_WIRE_FIELD(bytes, 4, p->base.key.tt_slot_id, encode);
	OP_WIRE_FIELD(bytes, 8, p->base.key.cluster_epoch, encode);
	OP_WIRE_FIELD(bytes, 12, p->base.key.local_xid, encode);
	OP_WIRE_FIELD(bytes, 16, p->base.key.segment_generation, encode);
	OP_WIRE_FIELD(bytes, 20, p->base.key.slot_wrap, encode);
	OP_WIRE_FIELD(bytes, 22, p->base.key.binding_version, encode);
	OP_WIRE_FIELD(bytes, 24, p->base.commit_scn, encode);
	OP_WIRE_FIELD(bytes, 32, p->base.member_xid, encode);
	OP_WIRE_FIELD(bytes, 36, p->base.member_ordinal, encode);
	OP_WIRE_FIELD(bytes, 38, p->base.member_status, encode);
	OP_WIRE_FIELD(bytes, 39, p->base.state, encode);
	if (encode)
		memcpy(bytes + 40, p->base.reserved8, 4);
	else
		memcpy(p->base.reserved8, bytes + 40, 4);
	OP_WIRE_FIELD(bytes, 48, p->operation_uba.raw[0], encode);
	OP_WIRE_FIELD(bytes, 56, p->operation_uba.raw[1], encode);
	OP_WIRE_FIELD(bytes, 64, p->top_fxid, encode);
	OP_WIRE_FIELD(bytes, 72, p->operation_fxid, encode);
	OP_WIRE_FIELD(bytes, 80, p->effect_state, encode);
	OP_WIRE_FIELD(bytes, 81, p->format_version, encode);
	OP_WIRE_FIELD(bytes, 82, p->flags, encode);
	OP_WIRE_FIELD(bytes, 84, p->reserved, encode);
}

static bool
operation_wire_proof_valid(const ClusterCurrentMemberOperationProof *p,
						   const ClusterCurrentMxOperationAsk *ask, int32 source, uint64 epoch)
{
	const ClusterCurrentMemberProofKey *key = &p->base.key;
	uint32 segment, block;
	uint16 slot, row;
	if (p->format_version != 2 || p->reserved != 0 || p->flags != (ask->exact ? 1 : 0)
		|| p->base.member_xid != ask->member.xid
		|| p->base.member_ordinal != ask->member.member_ordinal
		|| p->base.member_status != ask->member.member_status)
		return false;
	if (!ask->exact)
		return bytes_are_zero(&p->operation_uba, sizeof(p->operation_uba))
			   && U64FromFullTransactionId(p->top_fxid) == 0
			   && U64FromFullTransactionId(p->operation_fxid) == 0 && p->effect_state == 0
			   && (p->base.state != CCM_ACTIVE || key->local_xid == ask->member.xid)
			   && wire_member_proof_valid(&p->base, &ask->member, source, epoch);
	/* The request binds the actual DATA operation; canonical TT may reside in
  * another segment of this same origin. Its generation is supplied by the
  * qualified origin, never guessed from the DATA address. */
	if (memcmp(&p->operation_uba, &ask->operation.locator.uba, sizeof(p->operation_uba)) != 0
		|| !FullTransactionIdEquals(p->top_fxid, ask->operation.top_fxid)
		|| !FullTransactionIdEquals(p->operation_fxid, ask->operation.operation_fxid)
		|| (p->effect_state != CLUSTER_SAVEPOINT_EFFECT_LIVE
			&& p->effect_state != CLUSTER_SAVEPOINT_EFFECT_DISCARDED)
		|| !uba_decode_record(p->operation_uba, &segment, &block, &slot, &row)
		|| key->origin_node_id != source || key->undo_segment_id == 0
		|| (key->undo_segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE != source
		|| key->tt_slot_id != (uint32)slot + 1 || key->cluster_epoch != epoch
		|| key->local_xid != XidFromFullTransactionId(p->top_fxid)
		|| key->segment_generation == UINT32_MAX || key->slot_wrap != ask->operation.locator.tt_wrap
		|| key->binding_version != CLUSTER_CURRENT_MEMBER_PROOF_BINDING_VERSION)
		return false;
	switch (p->base.state) {
	case CCM_ACTIVE:
		return p->base.commit_scn == InvalidScn
			   && ClusterCurrentMemberProofGetCtrcGrant(&p->base) != 0;
	case CCM_COMMITTED:
		return SCN_VALID(p->base.commit_scn)
			   && ClusterCurrentMemberProofGetCtrcGrant(&p->base) == 0;
	case CCM_ABORTED:
		return p->base.commit_scn == InvalidScn
			   && ClusterCurrentMemberProofGetCtrcGrant(&p->base) == 0;
	default:
		return false;
	}
}

static bool
operation_wire_updater_valid(const ClusterCurrentUpdaterProof *proof,
							 const ClusterCurrentMxOperationForwardV6 *request)
{
	ClusterCurrentMxUpdaterChallengeWire up;
	memcpy(&up, request->body, sizeof(up));
	return proof != NULL && wire_mxkey_equal(&proof->mxkey, &request->prefix.mxkey)
		   && memcmp(&proof->candidate_next_xmin_alias, &up.candidate_next_xmin_alias,
					 sizeof(up.candidate_next_xmin_alias))
				  == 0
		   && proof->candidate_next_xmin_locator.tt_wrap <= TT_WRAP_MAX
		   && cluster_tx_locator_reply_matches(&up.candidate_next_xmin_locator,
											   &proof->candidate_next_xmin_locator)
		   && proof->updater_xid == up.updater_xid && proof->member_ordinal == up.member_ordinal
		   && proof->verdict < CUCP_UNKNOWN && proof->reserved8 == 0;
}

static void
operation_wire_outputs_reset(ClusterMxResolveResult *result,
							 ClusterCurrentMemberOperationProof *proofs, uint16 capacity,
							 uint16 *count, ClusterCurrentUpdaterProof *updater, uint32 *capability)
{
	if (result != NULL)
		*result = CMX_RESOLVE_UNKNOWN;
	if (proofs != NULL) {
		memset(proofs, 0, sizeof(*proofs) * capacity);
		for (uint16 i = 0; i < capacity; i++) {
			proofs[i].base.state = CCM_UNKNOWN;
			proofs[i].format_version = 2;
		}
	}
	if (count != NULL)
		*count = 0;
	if (capability != NULL)
		*capability = 0;
	if (updater != NULL) {
		memset(updater, 0, sizeof(*updater));
		updater->verdict = CUCP_UNKNOWN;
	}
}

bool
cluster_multixact_operation_wire_validate_reply(
	const void *payload, uint32 length, int32 source, uint64 epoch,
	const ClusterCurrentMxOperationForwardV6 *request, ClusterMxResolveResult *result,
	ClusterCurrentMemberOperationProof *proofs, uint16 capacity, uint16 *count,
	ClusterCurrentUpdaterProof *updater, uint32 *capability_generation)
{
	ClusterCurrentMxOperationForwardV6 checked;
	ClusterCurrentMxOperationReplyPageV6 page;
	ClusterCurrentMemberOperationProof staged[7] = { 0 };
	ClusterCurrentUpdaterProof staged_updater = { .verdict = CUCP_UNKNOWN };
	const ClusterCurrentMxProofReplyHeader *h = &page.header;
	Size used;
	operation_wire_outputs_reset(result, proofs, capacity, count, updater, capability_generation);
	if (payload == NULL || length != sizeof(page) || request == NULL || result == NULL
		|| count == NULL || updater == NULL || capability_generation == NULL
		|| !cluster_multixact_operation_wire_validate_forward(
			request, sizeof(*request), request->prefix.original_requester_node, source, epoch,
			&checked))
		return false;
	memcpy(&page, payload, sizeof(page));
	if (h->magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
		|| h->version != CLUSTER_CURRENT_MX_OPERATION_WIRE_VERSION
		|| h->kind != GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF || h->flags != 0
		|| h->source_node_id != (uint32)source || h->request_id != checked.prefix.request_id
		|| !wire_mxkey_equal(&h->mxkey, &checked.prefix.mxkey)
		|| h->descriptor_hash != ClusterCurrentMxProofPrefixGetDescriptorHash(&checked.prefix)
		|| h->total_count != checked.prefix.total_count
		|| h->chunk_ordinal != checked.prefix.chunk_ordinal
		|| h->chunk_count_minus_one != checked.prefix.chunk_count_minus_one
		|| h->result > CMX_RESOLVE_RETRY || h->result == CMX_RESOLVE_TIMEOUT || h->reserved16 != 0)
		return false;
	if (h->result != CMX_RESOLVE_OK) {
		if (h->entry_count != 0 || h->wire_length != 64 || h->requester_capability_generation != 0
			|| !bytes_are_zero((const uint8 *)&page + 64, sizeof(page) - 64))
			return false;
		*result = (ClusterMxResolveResult)h->result;
		return true;
	}
	used = 64 + checked.prefix.entry_count * 88;
	if (checked.prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE)
		used += sizeof(staged_updater);
	if (proofs == NULL || capacity < checked.prefix.entry_count
		|| h->entry_count != checked.prefix.entry_count || h->requester_capability_generation == 0
		|| h->wire_length != used
		|| !bytes_are_zero((const uint8 *)&page + used, sizeof(page) - used))
		return false;
	for (uint16 i = 0; i < h->entry_count; i++) {
		ClusterCurrentMxOperationAsk ask;
		uint8 *bytes = page.body + i * 88;
		if (!bytes_are_zero(bytes + 44, 4) || !operation_wire_read_ask(&checked, i, source, &ask))
			return false;
		operation_wire_proof_fields(bytes, &staged[i], false);
		if (!operation_wire_proof_valid(&staged[i], &ask, source, epoch))
			return false;
	}
	if (checked.prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE) {
		memcpy(&staged_updater, page.body + 88, sizeof(staged_updater));
		if (!operation_wire_updater_valid(&staged_updater, &checked))
			return false;
	}
	memcpy(proofs, staged, sizeof(*proofs) * h->entry_count);
	*updater = staged_updater;
	*count = h->entry_count;
	*capability_generation = h->requester_capability_generation;
	*result = CMX_RESOLVE_OK;
	return true;
}

bool
cluster_multixact_operation_wire_encode_reply(
	const ClusterCurrentMxOperationForwardV6 *request, int32 source, uint64 epoch,
	ClusterMxResolveResult result, const ClusterCurrentMemberOperationProof *proofs, uint16 count,
	const ClusterCurrentUpdaterProof *updater, uint32 capability_generation,
	ClusterCurrentMxOperationReplyPageV6 *out)
{
	ClusterCurrentMxOperationForwardV6 checked;
	ClusterCurrentMxProofReplyHeader *h;
	bool is_updater;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out == NULL || request == NULL || (unsigned)result > CMX_RESOLVE_RETRY
		|| result == CMX_RESOLVE_TIMEOUT
		|| !cluster_multixact_operation_wire_validate_forward(
			request, sizeof(*request), request->prefix.original_requester_node, source, epoch,
			&checked))
		return false;
	is_updater = checked.prefix.body_kind == CLUSTER_CURRENT_MX_PROOF_BODY_UPDATER_CHALLENGE;
	if (result != CMX_RESOLVE_OK) {
		if (count != 0 || capability_generation != 0 || updater != NULL)
			return false;
	} else {
		if (proofs == NULL || count != checked.prefix.entry_count || capability_generation == 0
			|| (is_updater ? !operation_wire_updater_valid(updater, &checked) : updater != NULL))
			return false;
		/* Validate every item before writing even the first output byte. */
		for (uint16 i = 0; i < count; i++) {
			ClusterCurrentMxOperationAsk ask;
			if (!operation_wire_read_ask(&checked, i, source, &ask)
				|| !operation_wire_proof_valid(&proofs[i], &ask, source, epoch))
				return false;
		}
	}
	h = &out->header;
	h->magic = CLUSTER_CURRENT_MX_WIRE_MAGIC;
	h->version = CLUSTER_CURRENT_MX_OPERATION_WIRE_VERSION;
	h->kind = GCS_BLOCK_FORWARD_KIND_CURRENT_MX_MEMBER_PROOF;
	h->result = result;
	h->source_node_id = source;
	h->request_id = checked.prefix.request_id;
	h->mxkey = checked.prefix.mxkey;
	h->descriptor_hash = ClusterCurrentMxProofPrefixGetDescriptorHash(&checked.prefix);
	h->total_count = checked.prefix.total_count;
	h->entry_count = count;
	h->chunk_ordinal = checked.prefix.chunk_ordinal;
	h->chunk_count_minus_one = checked.prefix.chunk_count_minus_one;
	h->wire_length = 64;
	h->requester_capability_generation = capability_generation;
	for (uint16 i = 0; i < count; i++) {
		ClusterCurrentMemberOperationProof proof = proofs[i];
		operation_wire_proof_fields(out->body + i * 88, &proof, true);
		h->wire_length += 88;
	}
	if (result == CMX_RESOLVE_OK && is_updater) {
		memcpy(out->body + 88, updater, sizeof(*updater));
		h->wire_length += sizeof(*updater);
	}
	return true;
}

#undef OP_WIRE_FIELD
