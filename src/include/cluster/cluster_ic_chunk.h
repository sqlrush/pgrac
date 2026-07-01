/*-------------------------------------------------------------------------
 *
 * cluster_ic_chunk.h
 *	  pgrac cluster IC chunked-payload framing (spec-2.4 D5).
 *
 *	  Sends payloads up to cluster.interconnect_payload_max_bytes
 *	  (default 64 MB;hard cap 256 MB) by splitting into N envelope
 *	  frames, each wrapped with msg_type=PGRAC_IC_CHUNK_MSG_TYPE (255)
 *	  and a 16-byte ClusterICChunkHeader at the front of the payload.
 *
 *	  Receiving side reassembles N chunks into a single buffer in a
 *	  per-peer dedicated AllocSetContext (per Q4 修订;
 *	  cluster_chunk_reassembly_ctx[CLUSTER_MAX_NODES] -- process-local,
 *	  L61 process-resource);complete reassembly invokes
 *	  cluster_ic_dispatch_envelope on inner_msg_type then atomic
 *	  cleanup via chunk_reset_peer().
 *
 *	  Single chunk_reset_peer(peer) call is the unified cleanup path
 *	  for ALL failure modes (timeout / close-peer / seq-error /
 *	  dispatch-complete / new-chunk-on-busy-slot).  Cleanup
 *	  correctness is a property of the design, not of individual
 *	  error paths (per Q4 / L72-style isolation).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ic_chunk.h
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode
 *	  (USE_PGRAC_CLUSTER);frontend tools (pg_waldump etc.) do not
 *	  need chunked support so the entire header is gated.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_IC_CHUNK_H
#define CLUSTER_IC_CHUNK_H

#ifndef FRONTEND

#include "c.h"

#include "cluster/cluster_ic_envelope.h"

/*
 * Reserved msg_type for chunk-wrap framing.  spec-2.3 enum has
 * slot 255 reserved for this.
 */
#define PGRAC_IC_CHUNK_MSG_TYPE ((uint8)255)

/*
 * Default + bounds for cluster.interconnect_payload_max_bytes (GUC).
 * 64 MB default is conservative;hard cap 256 MB caps reassembly
 * memory pressure (per spec §3.3 Q3).
 */
#define PGRAC_IC_PAYLOAD_MAX_DEFAULT (64 * 1024 * 1024)
#define PGRAC_IC_PAYLOAD_MAX_HARD_CAP (256 * 1024 * 1024)

/*
 * Per-frame payload bytes available AFTER envelope (36 B) +
 * chunk header (16 B) overhead.  At 16 MB envelope hard cap that's
 * 16777216 - 36 - 16 = 16777164 bytes.  Caller-supplied total length
 * is split into ceil(total / PGRAC_IC_CHUNK_BYTES) frames.
 */
#define PGRAC_IC_CHUNK_BYTES (PGRAC_IC_PAYLOAD_MAX - sizeof(ClusterICChunkHeader))

/*
 * spec-2.4 D5 wire fragment header.  Lives at the front of every
 * msg_type=255 payload, before the chunk byte slice.  16 bytes
 * packed (StaticAssertDecl in cluster_ic_chunk.c).
 */
typedef struct pg_attribute_packed() ClusterICChunkHeader
{
	uint32 chunk_seq;		  /* 0-based;offset 0;4 B */
	uint32 chunk_total;		  /* total chunks (>= 2);offset 4;4 B */
	uint32 total_payload_len; /* original payload bytes;offset 8;4 B */
	uint8 inner_msg_type;	  /* offset 12;1 B;1..254 */
	uint8 _pad[3];			  /* offset 13;3 B;write 0;ignored */
}
ClusterICChunkHeader;

/*
 * Send a payload of arbitrary length up to
 * cluster.interconnect_payload_max_bytes.  Returns true if all
 * chunks queued for send.  ereport(ERROR) on contract violation
 * (caller-side gate -- per Q3:GUC enforce at entry, NOT silent
 * truncate).
 */
extern bool cluster_ic_send_envelope_chunked(uint8 inner_msg_type, int32 dest_node_id,
											 const void *payload, size_t len);

/*
 * Receive-side dispatch hook.  cluster_ic_router::dispatch_envelope
 * recognises envelope.msg_type == PGRAC_IC_CHUNK_MSG_TYPE and
 * delegates to this entry point;single-chunk dispatches are NOT
 * routed here (msg_type 1..254 hits dispatch_table directly).
 *
 * Returns true on accepted frame (whether mid-stream or final);
 * false on contract violation (caller -- LMON tier1 -- closes peer).
 */
extern bool cluster_ic_chunk_dispatch_frame(const ClusterICEnvelope *env, const void *payload,
											int32 peer_id);

/*
 * Atomic cleanup for a peer's reassembly state.  Single call frees:
 *   - reassembly buf (palloc'd in dedicated context)
 *   - reassembly state struct fields
 *   - the per-peer MemoryContext itself
 *
 * Called from: chunk_reassembly_timeout scan (LMON main tick) /
 *              cluster_ic_tier1_close_peer / chunk seq mismatch /
 *              chunk dispatch complete / re-init on chunk_seq=0
 *              while slot was already occupied.
 *
 * Idempotent: noop when peer has no reassembly state.
 */
extern void cluster_ic_chunk_reset_peer(int32 peer_id);

/*
 * LMON main-tick hook to scan for reassembly timeouts.  GUC-driven
 * threshold (cluster.interconnect_chunk_reassembly_timeout_ms).
 * On timeout: bump per-peer counter + close peer + LOG `53R21`.
 *
 * Idempotent: noop when no peer has active reassembly.
 */
extern void cluster_ic_chunk_scan_reassembly_timeouts(void);

#endif /* !FRONTEND */
#endif /* CLUSTER_IC_CHUNK_H */
