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
#include "cluster/cluster_pcm_x_convert.h"
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
StaticAssertDecl(offsetof(PcmXWaitIdentity, tag) == 0, "PCM-X wait tag must lead payloads");
StaticAssertDecl(offsetof(PcmXTicketRef, identity) == 0,
				 "PCM-X ticket identity must lead payloads");
StaticAssertDecl(offsetof(PcmXEnqueuePayload, identity) == 0,
				 "PCM-X enqueue tag carrier must lead payload");
StaticAssertDecl(offsetof(PcmXAdmitAckPayload, ref) == 0, "PCM-X ref carrier must lead payload");
StaticAssertDecl(offsetof(PcmXBlockerChunkPayload, tag) == 0,
				 "PCM-X blocker tag must lead payload");

int
cluster_gcs_block_payload_shard(uint8 msg_type, const void *payload, uint16 payload_len,
								int n_workers)
{
	const BufferTag *tag;
	BufferTag pcm_x_tag;
	uint16 pcm_x_expected_len = 0;

	if (payload == NULL)
		return -1;

	switch (msg_type) {
	case PGRAC_IC_MSG_GCS_BLOCK_REQUEST:
		if (payload_len != sizeof(GcsBlockRequestPayload))
			return -1;
		tag = &((const GcsBlockRequestPayload *)payload)->tag;
		break;
	case PGRAC_IC_MSG_GCS_BLOCK_FORWARD:
		if (payload_len != sizeof(GcsBlockForwardPayload))
			return -1;
		tag = &((const GcsBlockForwardPayload *)payload)->tag;
		break;
	case PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE:
		if (payload_len != sizeof(GcsBlockInvalidatePayload))
			return -1;
		tag = &((const GcsBlockInvalidatePayload *)payload)->tag;
		break;
	case PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK:
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
		if (payload_len != sizeof(GcsBlockDonePayload))
			return -1;
		tag = &((const GcsBlockDonePayload *)payload)->tag;
		break;
	case PGRAC_IC_MSG_PCM_X_ENQUEUE:
		pcm_x_expected_len = sizeof(PcmXEnqueuePayload);
		break;
	case PGRAC_IC_MSG_PCM_X_ADMIT_ACK:
	case PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK:
		pcm_x_expected_len = sizeof(PcmXAdmitAckPayload);
		break;
	case PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM:
	case PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK:
	case PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK:
	case PGRAC_IC_MSG_PCM_X_COMMIT_X:
	case PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK:
	case PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM:
	case PGRAC_IC_MSG_PCM_X_CANCEL:
	case PGRAC_IC_MSG_PCM_X_CANCEL_ACK:
	case PGRAC_IC_MSG_PCM_X_DRAIN_ACK:
		pcm_x_expected_len = sizeof(PcmXPhasePayload);
		break;
	case PGRAC_IC_MSG_PCM_X_BLOCKER_SET_BEGIN:
	case PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT:
		pcm_x_expected_len = sizeof(PcmXBlockerSetHeaderPayload);
		break;
	case PGRAC_IC_MSG_PCM_X_BLOCKER_SET_EDGE:
		pcm_x_expected_len = sizeof(PcmXBlockerChunkPayload);
		break;
	case PGRAC_IC_MSG_PCM_X_REVOKE:
		/* Source-floor V2 appends one SCN to the byte-identical V1 prefix. */
		if (payload_len != sizeof(PcmXRevokePayload)
			&& payload_len != sizeof(PcmXRevokePayloadV2))
			return -1;
		memcpy(&pcm_x_tag, payload, sizeof(pcm_x_tag));
		tag = &pcm_x_tag;
		break;
	case PGRAC_IC_MSG_PCM_X_IMAGE_READY:
	case PGRAC_IC_MSG_PCM_X_PREPARE_GRANT:
		pcm_x_expected_len = sizeof(PcmXGrantPayload);
		break;
	case PGRAC_IC_MSG_PCM_X_INSTALL_READY:
		/* A' rebase: the V1 104-byte and V2 112-byte exact frames are both
		 * legal; the tag prefix is identical, so the shard key is too. */
		if (payload_len != sizeof(PcmXInstallReadyPayload)
			&& payload_len != PCM_X_INSTALL_READY_V1_LEN)
			return -1;
		memcpy(&pcm_x_tag, payload, sizeof(pcm_x_tag));
		tag = &pcm_x_tag;
		break;
	case PGRAC_IC_MSG_PCM_X_FINAL_ACK:
		pcm_x_expected_len = sizeof(PcmXFinalAckPayload);
		break;
	case PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL:
		pcm_x_expected_len = sizeof(PcmXPrehandleCancelPayload);
		break;
	case PGRAC_IC_MSG_PCM_X_DRAIN_POLL:
		pcm_x_expected_len = sizeof(PcmXDrainPollPayload);
		break;
	default:
		/* Tagless replies are direct-sent, not staged; any other DATA type
		 * needs an explicit shard key before it may enter an outbound ring. */
		return -1;
	}
	if (pcm_x_expected_len != 0) {
		if (payload_len != pcm_x_expected_len)
			return -1;
		memcpy(&pcm_x_tag, payload, sizeof(pcm_x_tag));
		tag = &pcm_x_tag;
	}

	return cluster_lms_shard_for_tag(tag, n_workers);
}

#endif /* USE_PGRAC_CLUSTER */
