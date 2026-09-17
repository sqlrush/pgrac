/*-------------------------------------------------------------------------
 *
 * cluster_gcs_block_shard.c
 *	  pgrac DATA-plane staging payload -> worker router — spec-7.3 D4
 *	  (pure layer; extracted from cluster_gcs_block.c at D9).
 *
 *	  cluster_gcs_block_payload_shard() picks the outbound ring (= DATA
 *	  worker) for a staged block-family frame by hashing its BufferTag
 *	  through cluster_lms_shard_for_tag().  This file has no PG-backend
 *	  dependencies beyond the wire-struct headers, so it links into the
 *	  standalone cluster_unit suite and the D9 routing truth table runs
 *	  against the REAL router (not a reimplementation).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_gcs_block_shard.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Spec: spec-7.3-lms-worker-pool.md (D4/D9).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_multixact_current_wire.h"
#include "cluster/cluster_resource_x_node_wire.h"
#include "cluster/cluster_terminal_ref_census.h"
#include "storage/buf_internals.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * cluster_gcs_block_payload_shard — spec-7.3 D4 (8.A).
 *
 *	Pick the DATA worker for a staged block-family frame by hashing its
 *	BufferTag.  The legacy GCS staging types carry the tag at a fixed offset;
 *	every staged PCM-X conversion payload starts with the tag through its
 *	identity/ref carrier.  Tagless replies and PCM-X RETIRE frames are sent
 *	directly by the receiving worker and never reach this function.
 *
 *	Returns the worker id in [0, n_workers), or -1 if the (msg_type, payload)
 *	pair carries no routable tag.  -1 is an 8.A fail-closed signal: an
 *	unroutable DATA frame must be REFUSED, never defaulted to a worker (that
 *	would break per-tag order).  The size check pins the payload ABI so a
 *	mismatched length can never read a tag from the wrong offset.
 */
/* spec-7.3 D4 (8.A) — the routing key is the tag at a fixed offset in each
 * staging-path payload;  pin the offsets so a struct change can't silently
 * move the tag and misroute (payload_shard reads &p->tag, but this makes the
 * assumption explicit + fails the build if a field is inserted before it). */
StaticAssertDecl(offsetof(GcsBlockRequestPayload, tag) == 16,
				 "spec-7.3 D4: GcsBlockRequestPayload.tag offset moved");
StaticAssertDecl(offsetof(GcsBlockForwardPayload, tag) == 16,
				 "spec-7.3 D4: GcsBlockForwardPayload.tag offset moved");
StaticAssertDecl(offsetof(GcsBlockInvalidatePayload, tag) == 16,
				 "spec-7.3 D4: GcsBlockInvalidatePayload.tag offset moved");
StaticAssertDecl(offsetof(GcsBlockInvalidateAckPayload, tag) == 16,
				 "spec-7.3 D4: GcsBlockInvalidateAckPayload.tag offset moved");
StaticAssertDecl(offsetof(GcsBlockDonePayload, tag) == 16,
				 "GCS-race round-2 review F4: GcsBlockDonePayload.tag offset moved");
static bool
cluster_resource_x_payload_route_tag(uint8 msg_type, const void *payload, uint16 payload_len,
									 BufferTag *tag)
{
	ResourceXDecodedFrame decoded;
	ResourceXWireReject reject;

	if (!cluster_resource_x_wire_decode(msg_type, payload, payload_len, &decoded, &reject))
		return false;
	*tag = decoded.common.logical_assertion.resource;
	return true;
}

static uint16
cluster_ctrc_route_get_u16_le(const uint8 *bytes)
{
	return (uint16)bytes[0] | ((uint16)bytes[1] << 8);
}

static uint32
cluster_ctrc_route_get_u32_le(const uint8 *bytes)
{
	return (uint32)bytes[0] | ((uint32)bytes[1] << 8) | ((uint32)bytes[2] << 16)
		   | ((uint32)bytes[3] << 24);
}

static uint64
cluster_ctrc_route_get_u64_le(const uint8 *bytes)
{
	return (uint64)cluster_ctrc_route_get_u32_le(bytes)
		   | ((uint64)cluster_ctrc_route_get_u32_le(bytes + 4) << 32);
}

/* The full authenticated selector is checked by the receiving handler.  The
 * staging router validates the closed wire domain and the exact fields used
 * to derive affinity, so a 136-byte collision cannot fall through to a
 * BufferTag route or split one request between workers. */
static bool
cluster_ctrc_payload_route_tag(const void *payload, uint16 payload_len, BufferTag *tag)
{
	const uint8 *bytes = (const uint8 *)payload;
	uint64 request_id;
	uint64 epoch;
	uint32 requester_node;
	uint32 requester_backend;

	if (payload == NULL || tag == NULL || payload_len != CLUSTER_CTRC_SEAL_REQUEST_BYTES
		|| cluster_ctrc_route_get_u16_le(bytes + 60) != 0
		|| bytes[62] != CLUSTER_CTRC_SELECTOR_VERSION || bytes[63] != CLUSTER_CTRC_FORWARD_KIND
		|| cluster_ctrc_route_get_u32_le(bytes + 64) != CLUSTER_CTRC_WIRE_MAGIC
		|| cluster_ctrc_route_get_u16_le(bytes + 68) != CLUSTER_CTRC_WIRE_VERSION
		|| cluster_ctrc_route_get_u16_le(bytes + 70) != CLUSTER_CTRC_SEAL_REQUEST_BYTES
		|| (bytes[55] != CTRC_SEAL_CLOSE_AND_CLEAN && bytes[55] != CTRC_SEAL_CERTIFICATE_COMMITTED)
		|| cluster_ctrc_route_get_u32_le(bytes + 128) == 0
		|| cluster_ctrc_route_get_u32_le(bytes + 132) != 0)
		return false;

	request_id = cluster_ctrc_route_get_u64_le(bytes);
	epoch = cluster_ctrc_route_get_u64_le(bytes + 8);
	requester_node = cluster_ctrc_route_get_u32_le(bytes + 36);
	requester_backend = cluster_ctrc_route_get_u32_le(bytes + 40);
	if (request_id == 0 || epoch == 0 || requester_node >= CLUSTER_CTRC_MAX_PARTICIPANTS
		|| requester_backend != (uint32)CLUSTER_CTRC_INTERNAL_ENDPOINT)
		return false;

	*tag = GcsBlockCurrentMxRouteTagMake(request_id, epoch, (int32)requester_node,
										 (int32)requester_backend);
	return true;
}

int
cluster_gcs_block_payload_shard(uint8 msg_type, const void *payload, uint16 payload_len,
								int n_workers)
{
	const BufferTag *tag;
	BufferTag route_tag;
	BufferTag resource_x_tag;

	if (payload == NULL)
		return -1;

	switch (msg_type) {
	case PGRAC_IC_MSG_GCS_BLOCK_REQUEST:
		if (payload_len == RESOURCE_X_CONTROL_V1_BYTES
			|| payload_len == RESOURCE_X_SHORT_V1_BYTES) {
			if (!cluster_resource_x_payload_route_tag(msg_type, payload, payload_len,
													  &resource_x_tag))
				return -1;
			tag = &resource_x_tag;
			break;
		}
		if (payload_len != sizeof(GcsBlockRequestPayload)
			&& payload_len != sizeof(ClusterR4CrRequestPayload))
			return -1;
		tag = &((const GcsBlockRequestPayload *)payload)->tag;
		break;
	case PGRAC_IC_MSG_GCS_BLOCK_REPLY:
		if (payload_len != RESOURCE_X_CONTROL_V1_BYTES && payload_len != RESOURCE_X_PROOF_V1_BYTES
			&& payload_len != RESOURCE_X_IMAGE_V1_BYTES)
			return -1;
		if (!cluster_resource_x_payload_route_tag(msg_type, payload, payload_len, &resource_x_tag))
			return -1;
		tag = &resource_x_tag;
		break;
	case PGRAC_IC_MSG_GCS_BLOCK_FORWARD:
		if (payload_len == CLUSTER_CTRC_SEAL_REQUEST_BYTES) {
			if (!cluster_ctrc_payload_route_tag(payload, payload_len, &route_tag))
				return -1;
			tag = &route_tag;
			break;
		}
		if (payload_len == CLUSTER_CURRENT_MX_DESCRIBE_FORWARD_SIZE) {
			const GcsBlockForwardPayload *current_mx = (const GcsBlockForwardPayload *)payload;
			const ClusterCurrentMxDescribeForwardV2 *frame
				= (const ClusterCurrentMxDescribeForwardV2 *)payload;

			if (!GcsBlockForwardPayloadIsCurrentMxRuntime(current_mx)
				|| frame->trailer.magic != CLUSTER_CURRENT_MX_WIRE_MAGIC
				|| frame->trailer.version != CLUSTER_CURRENT_MX_WIRE_VERSION
				|| frame->trailer.flags != CLUSTER_CURRENT_MX_WIRE_FLAGS_NONE)
				return -1;
			route_tag = GcsBlockCurrentMxRouteTagMake(current_mx->request_id, current_mx->epoch,
													  current_mx->original_requester_node,
													  current_mx->requester_backend_id);
			tag = &route_tag;
			break;
		}
		if (payload_len != sizeof(GcsBlockForwardPayload)
			&& payload_len != sizeof(ClusterR4CrForwardPayload))
			return -1;
		/* Spec 8.4 D4 kind-4 and the M4-scoped existing kind-2 extension
		 * share the cooperative worker-0 driver and must stay on DATA0.
		 * Kind-4 additionally requires its internal endpoint; kind-2 keeps
		 * the positive backend id of the physical R4_CR reply slot.  Every
		 * other exact FORWARD96 retains the legacy tag shard. */
		if (payload_len == sizeof(ClusterR4CrForwardPayload)) {
			const ClusterR4CrForwardPayload *r4 = (const ClusterR4CrForwardPayload *)payload;

			if (r4->extension.r4_version == CLUSTER_R4_WIRE_VERSION
				&& (r4->extension.r4_kind == CLUSTER_R4_WIRE_TX_RESOLVE
					|| (r4->base.requester_backend_id == CLUSTER_GCS_BLOCK_R4_INTERNAL_ENDPOINT
						&& r4->extension.r4_kind == CLUSTER_R4_WIRE_UNDO_DATA_FETCH)))
				return n_workers > 0 ? 0 : -1;
		}
		tag = &((const GcsBlockForwardPayload *)payload)->tag;
		break;
	case PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE:
		if (payload_len == RESOURCE_X_CONTROL_V1_BYTES) {
			if (!cluster_resource_x_payload_route_tag(msg_type, payload, payload_len,
													  &resource_x_tag))
				return -1;
			tag = &resource_x_tag;
			break;
		}
		if (payload_len != sizeof(GcsBlockInvalidatePayload))
			return -1;
		tag = &((const GcsBlockInvalidatePayload *)payload)->tag;
		break;
	case PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK:
		if (payload_len == RESOURCE_X_CONTROL_V1_BYTES
			|| payload_len == RESOURCE_X_PROOF_V1_BYTES) {
			if (!cluster_resource_x_payload_route_tag(msg_type, payload, payload_len,
													  &resource_x_tag))
				return -1;
			tag = &resource_x_tag;
			break;
		}
		if (payload_len != sizeof(GcsBlockInvalidateAckPayload))
			return -1;
		tag = &((const GcsBlockInvalidateAckPayload *)payload)->tag;
		break;
	case PGRAC_IC_MSG_GCS_BLOCK_DONE:
		/* GCS-race round-2 review F4: the completion proof is a staged
		 * tag-carrying frame like REQUEST -- without this case every DONE
		 * was refused (-1) at the ring and the whole RC-F chain sent
		 * nothing.  Same shard key as the REQUEST it retires, so it lands
		 * on the worker that owns the dedup entry. */
		if (payload_len == RESOURCE_X_CONTROL_V1_BYTES
			|| payload_len == RESOURCE_X_SHORT_V1_BYTES) {
			if (!cluster_resource_x_payload_route_tag(msg_type, payload, payload_len,
													  &resource_x_tag))
				return -1;
			tag = &resource_x_tag;
			break;
		}
		if (payload_len != sizeof(GcsBlockDonePayload))
			return -1;
		tag = &((const GcsBlockDonePayload *)payload)->tag;
		break;
	case PGRAC_IC_MSG_PCM_X_ENQUEUE:
	case PGRAC_IC_MSG_PCM_X_ADMIT_ACK:
	case PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK:
	case PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM:
	case PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK:
	case PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK:
	case PGRAC_IC_MSG_PCM_X_COMMIT_X:
	case PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK:
	case PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM:
	case PGRAC_IC_MSG_PCM_X_CANCEL:
	case PGRAC_IC_MSG_PCM_X_CANCEL_ACK:
	case PGRAC_IC_MSG_PCM_X_DRAIN_ACK:
	case PGRAC_IC_MSG_PCM_X_BLOCKER_SET_BEGIN:
	case PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT:
	case PGRAC_IC_MSG_PCM_X_BLOCKER_SET_EDGE:
	case PGRAC_IC_MSG_PCM_X_REVOKE:
	case PGRAC_IC_MSG_PCM_X_IMAGE_READY:
	case PGRAC_IC_MSG_PCM_X_PREPARE_GRANT:
	case PGRAC_IC_MSG_PCM_X_INSTALL_READY:
	case PGRAC_IC_MSG_PCM_X_FINAL_ACK:
	case PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL:
	case PGRAC_IC_MSG_PCM_X_DRAIN_POLL:
	case PGRAC_IC_MSG_PCM_X_RETIRE_UP_TO:
	case PGRAC_IC_MSG_PCM_X_RETIRE_ACK:
		/* Source-removed values are routed to the one bounded stale-family
		 * disposition.  No retired payload byte is parsed for a tag. */
		return n_workers > 0 ? 0 : -1;
	default:
		/* Tagless replies are direct-sent, not staged; any other DATA type
		 * needs an explicit shard key before it may enter an outbound ring. */
		return -1;
	}
	return cluster_lms_shard_for_tag(tag, n_workers);
}

#endif /* USE_PGRAC_CLUSTER */
