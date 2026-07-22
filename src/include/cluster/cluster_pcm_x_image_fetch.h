/*-------------------------------------------------------------------------
 *
 * cluster_pcm_x_image_fetch.h
 *	  PCM-X requester adapter for fetching a staged holder image through the
 *	  existing GCS BLOCK request/reply family.
 *
 * No page bytes are added to PCM-X types 50/51.  Their image_id is a
 * generation-exact handle into the holder's dedicated dedup record; the
 * established type-14/type-15 GCS channel carries the 48+8192 byte replay.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_pcm_x_image_fetch.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PCM_X_IMAGE_FETCH_H
#define CLUSTER_PCM_X_IMAGE_FETCH_H

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_pcm_x_bufmgr.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum PcmXImageFetchRequestRefusal {
	PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_NONE = 0,
	PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_ARGUMENT,
	PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_ENVELOPE,
	PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_REQUEST,
	PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_MASTER,
	PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_REF,
	PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_LEG,
	PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_HOLDER_IMAGE,
	PCM_X_IMAGE_FETCH_REQUEST_REFUSAL_REQUESTER_ID
} PcmXImageFetchRequestRefusal;

extern bool cluster_pcm_x_image_fetch_build_request(const PcmXLocalProgress *progress,
													int32 requester_node,
													int32 requester_backend_id,
													GcsBlockRequestPayload *request_out);
extern bool cluster_pcm_x_image_fetch_request_exact(const ClusterICEnvelope *env,
													const GcsBlockRequestPayload *request,
													const PcmXLocalHolderProgress *holder,
													int32 holder_node, int32 current_master_node,
													uint64 current_epoch);
extern bool cluster_pcm_x_image_fetch_request_exact_diagnosed(
	const ClusterICEnvelope *env, const GcsBlockRequestPayload *request,
	const PcmXLocalHolderProgress *holder, int32 holder_node, int32 current_master_node,
	uint64 current_epoch, PcmXImageFetchRequestRefusal *refusal_out);
extern bool cluster_pcm_x_image_fetch_reply_exact(const GcsBlockReplyHeader *reply,
												  const char *block_data,
												  const PcmXLocalProgress *progress,
												  int32 requester_node, int32 requester_backend_id);
extern bool cluster_pcm_x_image_fetch_reservation_exact(const ClusterPcmOwnSnapshot *live,
														const ClusterPcmOwnSnapshot *base,
														uint64 reservation_token);

/*
 * Called after ACTIVE_TRANSFER has begun the exact GRANT_PENDING reservation
 * and before INSTALL_READY is armed.  request_runtime pins every internal
 * backoff/reply wait and the install window to the formation that admitted the
 * writer.  The helper never commits or aborts the reservation; the queue
 * driver remains the sole owner of its lifecycle.
 */
extern PcmXQueueResult cluster_gcs_pcm_x_fetch_image_and_install(
	BufferDesc *buf, const PcmXLocalHandle *leader, const ClusterPcmOwnSnapshot *reservation_base,
	uint64 reservation_token, const PcmXRuntimeSnapshot *request_runtime);

/* Sole-requester S source: validate the already-resident immutable image and
 * atomically hand its exact REVOKING token to GRANT_PENDING.  No page bytes
 * are copied and no new reservation token is issued. */
extern PcmXQueueResult
cluster_gcs_pcm_x_adopt_self_image(BufferDesc *buf, const PcmXLocalHandle *leader,
								   const ClusterPcmOwnSnapshot *revoking_base,
								   uint64 *out_reservation_token);
/* COMMIT_X half of the same fused lifecycle.  Revalidate the page under
 * content EXCLUSIVE, then commit S->X with the protocol's sole generation
 * bump.  Once handoff occurred, failures preserve evidence and fail closed. */
extern PcmXQueueResult
cluster_gcs_pcm_x_finish_self_image_x(BufferDesc *buf, const PcmXLocalHandle *leader,
									  const ClusterPcmOwnSnapshot *revoking_base,
									  uint64 reservation_token, uint64 *out_committed_generation);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_PCM_X_IMAGE_FETCH_H */
