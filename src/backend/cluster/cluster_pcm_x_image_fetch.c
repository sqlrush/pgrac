/*-------------------------------------------------------------------------
 *
 * cluster_pcm_x_image_fetch.c
 *	  Dependency-light validation layer for the PCM-X holder-image adapter.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_pcm_x_image_fetch.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_pcm_x_image_fetch.h"
#include "storage/bufpage.h"


static bool
pcm_x_image_fetch_wait_identity_equal(const PcmXWaitIdentity *left, const PcmXWaitIdentity *right)
{
	return left != NULL && right != NULL && BufferTagsEqual(&left->tag, &right->tag)
		   && left->node_id == right->node_id && left->procno == right->procno
		   && left->xid == right->xid && left->cluster_epoch == right->cluster_epoch
		   && left->request_id == right->request_id && left->wait_seq == right->wait_seq
		   && left->base_own_generation == right->base_own_generation;
}


static bool
pcm_x_image_fetch_token_valid(const PcmXImageToken *image, int32 expected_master_node)
{
	int32 encoded_master_node;

	return image != NULL && image->source_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && expected_master_node >= 0 && expected_master_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && cluster_pcm_x_image_id_decode(image->image_id, &encoded_master_node, NULL)
		   && encoded_master_node == expected_master_node;
}


static bool
pcm_x_image_fetch_progress_valid(const PcmXLocalProgress *progress, int32 requester_node,
								 int32 requester_backend_id)
{
	int32 decoded_backend_id;
	int32 decoded_node;

	return progress != NULL && requester_node >= 0 && requester_node < PCM_X_PROTOCOL_NODE_LIMIT
		   && requester_backend_id > 0 && progress->identity.node_id == requester_node
		   && pcm_x_image_fetch_wait_identity_equal(&progress->identity, &progress->ref.identity)
		   && progress->ref.handle.ticket_id != 0 && progress->ref.handle.queue_generation != 0
		   && progress->ref.grant_generation != 0 && progress->identity.wait_seq != 0
		   && cluster_gcs_requester_id_decode(progress->identity.request_id, &decoded_node,
											  &decoded_backend_id, NULL)
		   && decoded_node == requester_node && decoded_backend_id == requester_backend_id
		   && progress->role == PCM_X_LOCAL_ROLE_NODE_LEADER
		   && progress->member_state == PCM_XL_REMOTE_WAIT && progress->pending_opcode == 0
		   && progress->last_response_opcode == PGRAC_IC_MSG_PCM_X_PREPARE_GRANT
		   && progress->master_session_incarnation != 0
		   && pcm_x_image_fetch_token_valid(&progress->image, progress->master_node);
}


bool
cluster_pcm_x_image_fetch_build_request(const PcmXLocalProgress *progress, int32 requester_node,
										int32 requester_backend_id,
										GcsBlockRequestPayload *request_out)
{
	if (request_out != NULL)
		memset(request_out, 0, sizeof(*request_out));
	if (request_out == NULL
		|| !pcm_x_image_fetch_progress_valid(progress, requester_node, requester_backend_id))
		return false;

	request_out->request_id = progress->image.image_id;
	request_out->epoch = progress->identity.cluster_epoch;
	request_out->tag = progress->identity.tag;
	request_out->sender_node = requester_node;
	request_out->requester_backend_id = requester_backend_id;
	request_out->transition_id = (uint8)PCM_TRANS_N_TO_S;
	return true;
}


bool
cluster_pcm_x_image_fetch_request_exact_diagnosed(const ClusterICEnvelope *env,
												  const GcsBlockRequestPayload *request,
												  const PcmXLocalHolderProgress *holder,
												  int32 holder_node, int32 current_master_node,
												  uint64 current_epoch,
												  PcmXImageFetchRequestRefusal *refusal_out)
{
	static const uint8 zero_reserved[sizeof(request->reserved_0)] = { 0 };
	int32 decoded_backend_id;
	int32 decoded_master_node;
	int32 decoded_requester_node;

	if (refusal_out != NULL)
		*refusal_out = PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_NONE;
	if (env == NULL || request == NULL || holder == NULL || holder_node < 0
		|| holder_node >= PCM_X_PROTOCOL_NODE_LIMIT || current_master_node < 0
		|| current_master_node >= PCM_X_PROTOCOL_NODE_LIMIT) {
		if (refusal_out != NULL)
			*refusal_out = PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_ARGUMENT;
		return false;
	}
	if (env->source_node_id != (uint32)request->sender_node
		|| env->dest_node_id != (uint32)holder_node || env->payload_length != sizeof(*request)) {
		if (refusal_out != NULL)
			*refusal_out = PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_ENVELOPE;
		return false;
	}
	if (request->sender_node < 0 || request->sender_node >= PCM_X_PROTOCOL_NODE_LIMIT
		|| request->requester_backend_id <= 0 || request->epoch != current_epoch
		|| request->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| memcmp(request->reserved_0, zero_reserved, sizeof(zero_reserved)) != 0) {
		if (refusal_out != NULL)
			*refusal_out = PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_REQUEST;
		return false;
	}
	if (holder->master_node != current_master_node || holder->master_session_incarnation == 0) {
		if (refusal_out != NULL)
			*refusal_out = PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_MASTER;
		return false;
	}
	if (holder->ref.grant_generation == 0 || holder->ref.handle.ticket_id == 0
		|| holder->ref.handle.queue_generation == 0
		|| holder->ref.identity.cluster_epoch != current_epoch || holder->ref.identity.wait_seq == 0
		|| !BufferTagsEqual(&holder->ref.identity.tag, &request->tag)
		|| holder->ref.identity.node_id != request->sender_node) {
		if (refusal_out != NULL)
			*refusal_out = PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_REF;
		return false;
	}
	if (holder->pending_opcode != PGRAC_IC_MSG_PCM_X_IMAGE_READY
		|| holder->phase != PGRAC_IC_MSG_PCM_X_IMAGE_READY || holder->flags != 0) {
		if (refusal_out != NULL)
			*refusal_out = PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_LEG;
		return false;
	}
	if (!pcm_x_image_fetch_token_valid(&holder->image, current_master_node)
		|| holder->image.image_id != request->request_id
		|| holder->image.source_node != (uint32)holder_node
		|| !cluster_pcm_x_image_id_decode(request->request_id, &decoded_master_node, NULL)
		|| decoded_master_node != current_master_node) {
		if (refusal_out != NULL)
			*refusal_out = PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_IMAGE;
		return false;
	}
	if (!cluster_gcs_requester_id_decode(holder->ref.identity.request_id, &decoded_requester_node,
										 &decoded_backend_id, NULL)
		|| decoded_requester_node != request->sender_node
		|| decoded_backend_id != request->requester_backend_id) {
		if (refusal_out != NULL)
			*refusal_out = PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_REQUESTER_ID;
		return false;
	}
	return true;
}


bool
cluster_pcm_x_image_fetch_request_exact(const ClusterICEnvelope *env,
										const GcsBlockRequestPayload *request,
										const PcmXLocalHolderProgress *holder, int32 holder_node,
										int32 current_master_node, uint64 current_epoch)
{
	return cluster_pcm_x_image_fetch_request_exact_diagnosed(
		env, request, holder, holder_node, current_master_node, current_epoch, NULL);
}


bool
cluster_pcm_x_image_fetch_reply_exact(const GcsBlockReplyHeader *reply, const char *block_data,
									  const PcmXLocalProgress *progress, int32 requester_node,
									  int32 requester_backend_id)
{
	static const uint8 zero_reserved[sizeof(reply->reserved_0)] = { 0 };
	PageHeaderData page_header;

	if (reply == NULL || block_data == NULL
		|| !pcm_x_image_fetch_progress_valid(progress, requester_node, requester_backend_id))
		return false;
	if (reply->request_id != progress->image.image_id || reply->page_lsn != progress->image.page_lsn
		|| reply->epoch != progress->identity.cluster_epoch
		|| reply->checksum != progress->image.page_checksum
		|| reply->sender_node != (int32)progress->image.source_node
		|| reply->requester_backend_id != requester_backend_id
		|| reply->transition_id != (uint8)PCM_TRANS_N_TO_S
		|| reply->status != (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER
		|| GcsBlockReplyHeaderGetForwardingMasterNode(reply) != GCS_BLOCK_REPLY_NO_FORWARDING_MASTER
		|| memcmp(reply->reserved_0, zero_reserved, sizeof(zero_reserved)) != 0
		|| cluster_gcs_block_compute_checksum(block_data) != progress->image.page_checksum)
		return false;

	memcpy(&page_header, block_data, sizeof(page_header));
	return (uint64)PageXLogRecPtrGet(page_header.pd_lsn) == progress->image.page_lsn
		   && (uint64)page_header.pd_block_scn == progress->image.page_scn;
}


bool
cluster_pcm_x_image_fetch_reservation_exact(const ClusterPcmOwnSnapshot *live,
											const ClusterPcmOwnSnapshot *base,
											uint64 reservation_token)
{
	/* Self-source S handoff validates its already-resident immutable bytes and
	 * never enters the network fetch/copy path. */
	return cluster_pcm_x_grant_reservation_kind(live, base, reservation_token)
		   == CLUSTER_PCM_X_GRANT_RESERVATION_N_NEW;
}

#endif /* USE_PGRAC_CLUSTER */
