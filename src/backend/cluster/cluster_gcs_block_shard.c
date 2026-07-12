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
#include "storage/buf_internals.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * cluster_gcs_block_payload_shard — spec-7.3 D4 (8.A).
 *
 *	Pick the DATA worker for a staged block-family frame by hashing its
 *	BufferTag.  Only the four tag-carrying staging-path types reach the
 *	outbound ring (REQUEST / FORWARD / INVALIDATE / DONE — each with the tag at a
 *	fixed offset);  REPLY (no tag, request_id-correlated) and INVALIDATE-ACK
 *	are sent DIRECTLY from the receiving worker's dispatch handler, so they
 *	already ride the correct worker channel and never reach this function.
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
StaticAssertDecl(offsetof(GcsBlockDonePayload, tag) == 16,
				 "GCS-race round-2 review F4: GcsBlockDonePayload.tag offset moved");

int
cluster_gcs_block_payload_shard(uint8 msg_type, const void *payload, uint16 payload_len,
								int n_workers)
{
	const BufferTag *tag;

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
	default:
		/* REPLY / INVALIDATE-ACK are direct-sent, not staged;  any other
		 * DATA type would need an explicit shard key (spec-7.3 §3.6). */
		return -1;
	}

	return cluster_lms_shard_for_tag(tag, n_workers);
}

#endif /* USE_PGRAC_CLUSTER */
