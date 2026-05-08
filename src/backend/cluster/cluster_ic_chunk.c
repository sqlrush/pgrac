/*-------------------------------------------------------------------------
 *
 * cluster_ic_chunk.c
 *	  pgrac cluster IC chunked-payload framing implementation
 *	  (spec-2.4 D6).
 *
 *	  Sends payloads up to cluster.interconnect_payload_max_bytes by
 *	  splitting into N envelope frames (msg_type=255 + ChunkHeader +
 *	  chunk slice).  Receiving side reassembles into a per-peer
 *	  dedicated AllocSetContext;single chunk_reset_peer(peer) is
 *	  the unified cleanup path for ALL failure modes.
 *
 *	  spec-2.4 期 only spec-2.13 GES large-payload caller will use
 *	  this;LMON heartbeat and other small-payload msg_types still
 *	  go through cluster_ic_send_envelope (single-frame, no chunking).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ic_chunk.c
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <string.h>

#include "datatype/timestamp.h"
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES */
#include "cluster/cluster_guc.h"  /* cluster_node_id */
#include "cluster/cluster_ic.h"	  /* cluster_ic_send_bytes */
#include "cluster/cluster_ic_chunk.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h" /* counter bumpers */


StaticAssertDecl(sizeof(ClusterICChunkHeader) == 16,
				 "ClusterICChunkHeader must be exactly 16 bytes (spec-2.4 §2.2 frozen)");
StaticAssertDecl(offsetof(ClusterICChunkHeader, chunk_seq) == 0,
				 "ClusterICChunkHeader.chunk_seq offset frozen at 0");
StaticAssertDecl(offsetof(ClusterICChunkHeader, chunk_total) == 4,
				 "ClusterICChunkHeader.chunk_total offset frozen at 4");
StaticAssertDecl(offsetof(ClusterICChunkHeader, total_payload_len) == 8,
				 "ClusterICChunkHeader.total_payload_len offset frozen at 8");
StaticAssertDecl(offsetof(ClusterICChunkHeader, inner_msg_type) == 12,
				 "ClusterICChunkHeader.inner_msg_type offset frozen at 12");


/* spec-2.4 D9: GUC -- value lives in cluster_guc.{h,c}.  Default
 * is PGRAC_IC_PAYLOAD_MAX_DEFAULT (64 MB);bounds 16 MB ~ 256 MB. */
extern int cluster_interconnect_payload_max_bytes;
extern int cluster_interconnect_chunk_reassembly_timeout_ms;


/* ============================================================
 * Per-peer reassembly state (process-local, L61).
 *
 *   per Q4 修订:each peer has a dedicated AllocSetContext + a
 *   small state struct.  Lazy-created on first chunk_seq=0 from
 *   peer.  Atomic-deleted on chunk_reset_peer().
 *
 *   Cleanup correctness comes from the design (delete the context),
 *   not from each error path remembering to pfree(buf).
 * ============================================================ */

typedef struct ChunkReassemblyState {
	uint8 *buf; /* allocated inside the per-peer ctx */
	uint32 total_payload_len;
	uint32 chunk_total;
	uint32 seq_next;
	uint8 inner_msg_type;
	int32 source_node_id;
	TimestampTz started_at;
} ChunkReassemblyState;

static MemoryContext cluster_chunk_reassembly_ctx[CLUSTER_MAX_NODES];
static ChunkReassemblyState cluster_chunk_reassembly_state[CLUSTER_MAX_NODES];


void
cluster_ic_chunk_reset_peer(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;

	if (cluster_chunk_reassembly_ctx[peer_id] != NULL) {
		MemoryContextDelete(cluster_chunk_reassembly_ctx[peer_id]);
		cluster_chunk_reassembly_ctx[peer_id] = NULL;
	}
	memset(&cluster_chunk_reassembly_state[peer_id], 0,
		   sizeof(cluster_chunk_reassembly_state[peer_id]));
	cluster_ic_tier1_set_chunk_reassembly_active(peer_id, 0);
}


/* ============================================================
 * Send path.
 * ============================================================ */

bool
cluster_ic_send_envelope_chunked(uint8 inner_msg_type, int32 dest_node_id, const void *payload,
								 size_t len)
{
	uint32 max_bytes;
	uint32 chunk_total;
	uint32 i;
	const uint8 *src = (const uint8 *)payload;
	uint8 *frame_buf;
	uint32 frame_buf_size;

	if (inner_msg_type == 0 || inner_msg_type == PGRAC_IC_CHUNK_MSG_TYPE)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_ic_send_envelope_chunked: invalid inner_msg_type %u",
							   inner_msg_type)));

	if (len > 0 && payload == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_ic_send_envelope_chunked: payload is NULL but len = %zu", len)));

	max_bytes = (uint32)cluster_interconnect_payload_max_bytes;
	if (len > (size_t)max_bytes)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("cluster_ic chunked payload %zu bytes exceeds GUC "
							   "cluster.interconnect_payload_max_bytes = %u",
							   len, max_bytes),
						errhint("Increase cluster.interconnect_payload_max_bytes or "
								"split the payload at the caller (hard cap %u bytes).",
								PGRAC_IC_PAYLOAD_MAX_HARD_CAP)));

	if (len <= PGRAC_IC_CHUNK_BYTES) {
		/*
		 * Small payload: single frame via send_envelope, no chunking.
		 * Caller wants chunked semantics but len fits in one frame --
		 * pass through.  (Tests may call with len=0 just to validate
		 * the call sequence;single send_envelope works.)
		 */
		ClusterICSendResult rc;

		rc = cluster_ic_send_envelope(inner_msg_type, dest_node_id, payload, (uint32)len);
		return rc == CLUSTER_IC_SEND_DONE;
	}

	chunk_total = (uint32)((len + PGRAC_IC_CHUNK_BYTES - 1) / PGRAC_IC_CHUNK_BYTES);
	frame_buf_size = sizeof(ClusterICChunkHeader) + PGRAC_IC_CHUNK_BYTES;
	frame_buf = palloc(frame_buf_size);

	for (i = 0; i < chunk_total; i++) {
		ClusterICChunkHeader hdr;
		size_t off;
		size_t this_chunk;
		ClusterICSendResult rc;

		off = (size_t)i * PGRAC_IC_CHUNK_BYTES;
		this_chunk = (i + 1 == chunk_total) ? (len - off) : PGRAC_IC_CHUNK_BYTES;

		memset(&hdr, 0, sizeof(hdr));
		hdr.chunk_seq = i;
		hdr.chunk_total = chunk_total;
		hdr.total_payload_len = (uint32)len;
		hdr.inner_msg_type = inner_msg_type;

		memcpy(frame_buf, &hdr, sizeof(hdr));
		memcpy(frame_buf + sizeof(hdr), src + off, this_chunk);

		rc = cluster_ic_send_envelope(PGRAC_IC_CHUNK_MSG_TYPE, dest_node_id, frame_buf,
									  (uint32)(sizeof(hdr) + this_chunk));
		if (rc != CLUSTER_IC_SEND_DONE) {
			pfree(frame_buf);
			return false;
		}
	}

	pfree(frame_buf);
	return true;
}


/* ============================================================
 * Receive path.
 * ============================================================ */

bool
cluster_ic_chunk_dispatch_frame(const ClusterICEnvelope *env, const void *payload, int32 peer_id)
{
	ClusterICChunkHeader hdr;
	const uint8 *body;
	uint32 body_len;
	ChunkReassemblyState *st;

	if (env == NULL || env->payload_length < sizeof(ClusterICChunkHeader))
		return false;
	if (payload == NULL)
		return false;
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;

	memcpy(&hdr, payload, sizeof(hdr));
	body = (const uint8 *)payload + sizeof(hdr);
	body_len = env->payload_length - (uint32)sizeof(hdr);

	if (hdr.chunk_total < 2)
		return false; /* single-chunk caller should NOT use chunked wrapper */
	if (hdr.inner_msg_type == 0 || hdr.inner_msg_type == PGRAC_IC_CHUNK_MSG_TYPE)
		return false; /* invalid inner type */
	if (hdr.total_payload_len > (uint32)cluster_interconnect_payload_max_bytes)
		return false; /* exceeds GUC */
	if (hdr.chunk_seq >= hdr.chunk_total)
		return false;

	st = &cluster_chunk_reassembly_state[peer_id];

	if (hdr.chunk_seq == 0) {
		MemoryContext oldctx;

		if (cluster_chunk_reassembly_ctx[peer_id] != NULL) {
			/* Slot already occupied;peer is misbehaving (sent new
			 * chunk_seq=0 before completing previous reassembly). */
			cluster_ic_chunk_reset_peer(peer_id);
			return false;
		}

		cluster_chunk_reassembly_ctx[peer_id] = AllocSetContextCreate(
			TopMemoryContext, "cluster chunk reassembly", ALLOCSET_DEFAULT_SIZES);
		oldctx = MemoryContextSwitchTo(cluster_chunk_reassembly_ctx[peer_id]);
		st->buf = palloc(hdr.total_payload_len);
		MemoryContextSwitchTo(oldctx);

		st->total_payload_len = hdr.total_payload_len;
		st->chunk_total = hdr.chunk_total;
		st->inner_msg_type = hdr.inner_msg_type;
		st->source_node_id = (int32)env->source_node_id;
		st->started_at = GetCurrentTimestamp();
		st->seq_next = 0;
		cluster_ic_tier1_set_chunk_reassembly_active(peer_id, hdr.chunk_total);
	} else {
		if (cluster_chunk_reassembly_ctx[peer_id] == NULL)
			return false; /* mid-stream chunk without seq=0 init */
		if (hdr.chunk_total != st->chunk_total || hdr.total_payload_len != st->total_payload_len
			|| hdr.inner_msg_type != st->inner_msg_type) {
			cluster_ic_chunk_reset_peer(peer_id);
			return false;
		}
	}

	if (hdr.chunk_seq != st->seq_next) {
		cluster_ic_chunk_reset_peer(peer_id);
		return false;
	}

	{
		size_t off = (size_t)hdr.chunk_seq * PGRAC_IC_CHUNK_BYTES;
		size_t expected_chunk_bytes = (hdr.chunk_seq + 1 == hdr.chunk_total)
										  ? (st->total_payload_len - off)
										  : PGRAC_IC_CHUNK_BYTES;

		if ((uint32)body_len != (uint32)expected_chunk_bytes) {
			cluster_ic_chunk_reset_peer(peer_id);
			return false;
		}
		memcpy(st->buf + off, body, body_len);
	}

	st->seq_next++;

	if (st->seq_next == st->chunk_total) {
		/*
		 * Reassembly complete.  Synthesize an inner envelope and
		 * dispatch it through the registered handler.  The inner
		 * envelope is built locally (CRC over the inner content) so
		 * the dispatch contract is identical to single-frame path.
		 *
		 * Note: dispatch handler runs in the per-dispatch
		 * AllocSetContext (spec-2.3 v1.0.1 F5 / L72) -- ERROR there
		 * does NOT clobber our per-peer reassembly_ctx.
		 */
		ClusterICEnvelope inner;
		bool dispatched;

		if (!cluster_ic_envelope_build(&inner, st->inner_msg_type, (uint32)st->source_node_id,
									   (uint32)cluster_node_id, st->buf, st->total_payload_len)) {
			cluster_ic_chunk_reset_peer(peer_id);
			return false;
		}
		dispatched = cluster_ic_dispatch_envelope(&inner, st->buf);
		cluster_ic_chunk_reset_peer(peer_id);
		return dispatched;
	}

	cluster_ic_tier1_set_chunk_reassembly_active(peer_id, st->chunk_total - st->seq_next);
	return true;
}


/* ============================================================
 * Reassembly-timeout scan (LMON main tick).
 * ============================================================ */

void
cluster_ic_chunk_scan_reassembly_timeouts(void)
{
	int peer;
	TimestampTz now = GetCurrentTimestamp();

	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		ChunkReassemblyState *st = &cluster_chunk_reassembly_state[peer];
		long secs;
		int usecs;
		long elapsed_ms;

		if (cluster_chunk_reassembly_ctx[peer] == NULL || st->started_at == 0)
			continue;

		TimestampDifference(st->started_at, now, &secs, &usecs);
		elapsed_ms = secs * 1000L + usecs / 1000;

		if (elapsed_ms > cluster_interconnect_chunk_reassembly_timeout_ms) {
			cluster_ic_tier1_bump_chunk_reassembly_timeout((int32)peer);
			ereport(WARNING,
					(errcode(ERRCODE_CLUSTER_IC_CHUNK_REASSEMBLY_TIMEOUT),
					 errmsg("cluster_ic chunk reassembly timeout for peer %d "
							"(elapsed %ld ms > %d ms threshold)",
							peer, elapsed_ms, cluster_interconnect_chunk_reassembly_timeout_ms)));
			cluster_ic_chunk_reset_peer((int32)peer);
			/*
			 * spec-2.4 D8 (already shipped in Step 4):
			 * cluster_ic_tier1_close_peer's hook calls chunk_reset_peer;
			 * here we close the peer to force reconnection on timeout.
			 */
			cluster_ic_tier1_close_peer((int32)peer, "chunk reassembly timeout");
		}
	}
}

#endif /* USE_PGRAC_CLUSTER */
