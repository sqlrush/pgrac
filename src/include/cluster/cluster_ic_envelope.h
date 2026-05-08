/*-------------------------------------------------------------------------
 *
 * cluster_ic_envelope.h
 *	  pgrac cluster IC wire envelope ABI (spec-2.3 D1; frontend-safe).
 *
 *	  spec-2.0 §4 frozen ABI -- 36-byte ClusterICEnvelope wraps every
 *	  cross-node message (heartbeat / SCN broadcast / GES / cache fusion
 *	  / sinval / fence / reconfig).  Header layout is anchored by 4
 *	  StaticAssertDecl in cluster_ic_envelope.c; any field reordering
 *	  is a wire ABI break that requires catversion bump (per L46) +
 *	  envelope.version field bump (per spec-2.3 §3.7 anti-mixed-wire
 *	  4-layer defense).
 *
 *	  Field semantics:
 *	    magic           -- PGRAC_IC_ENVELOPE_MAGIC; anti-garbage stream
 *	    version         -- PGRAC_IC_ENVELOPE_VERSION_V1; recv-time reject
 *	                       on mismatch (anti-mixed-wire defense layer 1)
 *	    msg_type        -- ClusterICMsgType enum; subsystem-defined,
 *	                       registered via cluster_ic_register_msg_type
 *	                       (spec-2.3 D4 router; phase 1 of postmaster)
 *	    source_node_id  -- sender's cluster.node_id
 *	    dest_node_id    -- receiver; PGRAC_IC_BROADCAST = 0xFFFFFFFF
 *	    epoch           -- spec-2.3: writer writes 0, receiver reads but
 *	                       NOT enforced.  spec-2.4 flips enforce flag
 *	                       (membership epoch reject).
 *	    scn             -- spec-2.3: writer writes 0, receiver reads.
 *	                       spec-2.10 wires walwriter SCN piggyback
 *	                       (Lamport advance per AD-008 / L21).
 *	    payload_length  -- bytes after envelope; ceiling 16 MB
 *	                       (PGRAC_IC_PAYLOAD_MAX).  spec-2.4 framing
 *	                       lifts via chunked send.
 *	    payload_crc32c  -- CRC32C over (envelope[0..32] excluding crc
 *	                       field at offset 32) + payload[0..length].
 *	                       Per spec-2.3 §3.3 + Q3 boundary clarification:
 *	                       empty payload still produces nonzero CRC
 *	                       because envelope header has multi-byte
 *	                       content (magic alone is 2 nonzero bytes).
 *
 *	  L8 frontend-safe: this header includes only "c.h" (uint{8,16,32,64}
 *	  + bool typedefs).  No backend-only types.  pg_waldump-style
 *	  frontend tools may #include this header to parse envelope bytes
 *	  even on --disable-cluster builds.  Backend-only API
 *	  (build/verify functions) is gated #ifndef FRONTEND.
 *
 *	  Spec authority: pgrac:specs/spec-2.3-envelope-abi-ratify-
 *	  transport-agnostic-api.md frozen v0.2 (2026-05-07; user
 *	  approve Q1-Q14 + 4 hard 修订).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ic_envelope.h
 *
 * NOTES
 *	  pgrac-original file.  Built always (frontend + backend).  The
 *	  ClusterICEnvelope struct definition is frontend-visible so cross-
 *	  version diagnostic tools can parse on-wire bytes.  C function
 *	  bodies in cluster_ic_envelope.c are gated by USE_PGRAC_CLUSTER
 *	  (--enable-cluster only).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_IC_ENVELOPE_H
#define CLUSTER_IC_ENVELOPE_H

#include "c.h" /* uint8 / uint16 / uint32 / uint64 / bool */


/* ============================================================
 * On-wire constants (frozen per spec-2.0 §4 + spec-2.3 v0.2).
 * ============================================================ */

/*
 * "IC" little-endian; first 2 bytes of every envelope on the wire.
 * Anti-garbage-stream guard.  StaticAssertDecl in .c locks
 * sizeof/offsets; magic itself is never changed (spec-2.0 §4 frozen).
 */
#define PGRAC_IC_ENVELOPE_MAGIC ((uint16)0x4943)

/*
 * Wire protocol version.  Bumped when ClusterICEnvelope shape changes
 * (catversion + StaticAssertDecl + recv-time reject all configured per
 * spec-2.3 §3.7 anti-mixed-wire 4-layer defense).  spec-2.3 baseline
 * is V1.
 */
#define PGRAC_IC_ENVELOPE_VERSION_V1 ((uint8)1)

/*
 * Reserved dest_node_id meaning "all peers".  spec-2.3 ships HEARTBEAT
 * with broadcast_ok=false (point-to-point only); future SCN_BROADCAST
 * (spec-2.10) / SINVAL (spec-2.18) / FENCE_NOTIFY (spec-2.28) will
 * register with broadcast_ok=true.
 */
#define PGRAC_IC_BROADCAST ((uint32)0xFFFFFFFFU)

/*
 * Compile-time constant for envelope size; tests + StaticAssertDecl
 * cross-check against sizeof(ClusterICEnvelope).
 */
#define PGRAC_IC_ENVELOPE_BYTES 36

/*
 * Hard ceiling on payload_length per spec-2.3 §3.5b.  Outbound API
 * (cluster_ic_send_envelope) ereport(ERROR) on exceed; inbound remote
 * frame > PAYLOAD_MAX = peer-level failure (close peer + LOG/WARN +
 * metric; never ERROR LMON).  spec-2.4 framing lifts via chunked send.
 */
#define PGRAC_IC_PAYLOAD_MAX (16 * 1024 * 1024)

/*
 * Maximum msg_type enum value; dispatch_table[] sized to this.  msg_type
 * 0 is reserved as the "unassigned" sentinel (handler == NULL by
 * definition; never registered).  Values 10..255 available for future
 * sub-spec; never reuse 0..9.
 */
#define CLUSTER_IC_MSG_TYPE_MAX 256


/* ============================================================
 * ClusterICMsgType enum.
 *
 *   Each value is a stable wire-ABI assignment; once assigned the
 *   value is reserved across ALL future specs.  Values for spec-2.X
 *   sub-specs are pre-allocated here so no two sub-specs collide.
 * ============================================================ */

typedef enum ClusterICMsgType {
	PGRAC_IC_MSG_RESERVED_0 = 0,	/* sentinel; never assigned */
	PGRAC_IC_MSG_HEARTBEAT = 1,		/* spec-2.2/2.3; LMON-only */
	PGRAC_IC_MSG_SCN_BROADCAST = 2, /* spec-2.10 reserved */
	PGRAC_IC_MSG_BOC_BROADCAST = 3, /* spec-2.10 reserved */
	PGRAC_IC_MSG_GES_REQUEST = 4,	/* spec-2.13 reserved */
	PGRAC_IC_MSG_GES_REPLY = 5,		/* spec-2.13 reserved */
	PGRAC_IC_MSG_CF_BLOCK_SHIP = 6, /* spec-2.16 reserved */
	PGRAC_IC_MSG_SINVAL = 7,		/* spec-2.18 reserved */
	PGRAC_IC_MSG_FENCE_NOTIFY = 8,	/* spec-2.28 reserved */
	PGRAC_IC_MSG_RECONFIG = 9		/* spec-2.29 reserved */
	/* values 10..255 available for future sub-spec; never reuse 0..9 */
} ClusterICMsgType;


/* ============================================================
 * ClusterICEnvelope -- 36-byte fixed wire ABI per spec-2.0 §4.
 *
 *   Field offset layout (anchored by StaticAssertDecl in
 *   cluster_ic_envelope.c):
 *
 *     [ 0,  2)  magic           uint16 LE
 *     [ 2,  3)  version         uint8
 *     [ 3,  4)  msg_type        uint8
 *     [ 4,  8)  source_node_id  uint32 LE
 *     [ 8, 12)  dest_node_id    uint32 LE
 *     [12, 20)  epoch           uint64 LE
 *     [20, 28)  scn             uint64 LE
 *     [28, 32)  payload_length  uint32 LE
 *     [32, 36)  payload_crc32c  uint32 LE
 *
 *   Natural alignment on 64-bit platforms (uint64 fields at offsets
 *   12 and 20 are 8-byte aligned via offset-12 = 8+4 and offset-20 =
 *   16+4).  L34: cross-platform receivers MUST memcpy() into a local
 *   uint64 before reading -- raw cast across alignment-strict
 *   ARM/SPARC platforms triggers SIGBUS.  cluster_ic_envelope_verify
 *   is alignment-safe via direct struct member access (compiler
 *   inserts memcpy when the source is misaligned).
 * ============================================================ */

/*
 * Note on packing: spec-2.0 §4 frozen offsets put epoch at offset 12
 * and scn at offset 20.  Both are uint64 fields whose offsets are NOT
 * 8-byte naturally aligned (12 % 8 == 4; 20 % 8 == 4).  Without
 * pg_attribute_packed(), the compiler inserts 4 bytes of padding
 * before epoch and grows sizeof to 40 bytes -- violating the wire ABI.
 *
 * pg_attribute_packed() forces the struct to honor exactly the
 * declared offsets.  L34 unaligned access risk: on alignment-strict
 * platforms (ARM/SPARC) reading env->epoch / env->scn directly may
 * emit alignment-trap code; the compiler handles this via memcpy
 * under the hood for packed structs.  cluster_ic_envelope.c verify /
 * compute_crc paths use direct member access and are alignment-safe
 * under -fpacked.
 */
typedef struct ClusterICEnvelope {
	uint16 magic;		   /* offset 0;  PGRAC_IC_ENVELOPE_MAGIC */
	uint8 version;		   /* offset 2;  PGRAC_IC_ENVELOPE_VERSION_V1 */
	uint8 msg_type;		   /* offset 3;  ClusterICMsgType */
	uint32 source_node_id; /* offset 4 */
	uint32 dest_node_id;   /* offset 8;  PGRAC_IC_BROADCAST = 0xFFFFFFFF */
	uint64 epoch;		   /* offset 12; spec-2.4 enforce */
	uint64 scn;			   /* offset 20; spec-2.10 piggyback */
	uint32 payload_length; /* offset 28; <= PGRAC_IC_PAYLOAD_MAX */
	uint32 payload_crc32c; /* offset 32; CRC over (env-excl-crc) + payload */
} pg_attribute_packed() ClusterICEnvelope;


/* ============================================================
 * Public C API (build / verify / CRC compute; backend-only).
 *
 *   Gated #ifndef FRONTEND so frontend diagnostic tools can
 *   #include this header for the struct definition without
 *   pulling in unresolved symbols.
 * ============================================================ */

#ifndef FRONTEND

/*
 * cluster_ic_envelope_build -- fill an outbound envelope.
 *
 *   out_env         must be a caller-allocated ClusterICEnvelope.
 *   msg_type        ClusterICMsgType; caller is expected to have
 *                   registered via cluster_ic_register_msg_type
 *                   (spec-2.3 D4) before send.
 *   source_node_id  cluster.node_id of sender.
 *   dest_node_id    receiver, or PGRAC_IC_BROADCAST.
 *   payload         pointer to payload bytes (may be NULL if length 0).
 *   payload_length  bytes in payload; must be <= PGRAC_IC_PAYLOAD_MAX.
 *
 *   Both epoch and scn are written as 0 in spec-2.3 (per §3.2 + Q12);
 *   spec-2.4 / 2.10 will rewrite to actual values when their enforce
 *   flags flip.  payload_crc32c is computed last over (env[0..32] +
 *   payload).
 *
 *   Returns true on success, false if payload_length exceeds
 *   PGRAC_IC_PAYLOAD_MAX (caller is expected to ereport ERROR per
 *   spec-2.3 §3.5b outbound 16MB rule -- this function does not
 *   ereport so it stays usable from contexts where ereport is unsafe).
 */
extern bool cluster_ic_envelope_build(ClusterICEnvelope *out_env, uint8 msg_type,
									  uint32 source_node_id, uint32 dest_node_id,
									  const void *payload, uint32 payload_length);

/*
 * cluster_ic_envelope_verify -- validate an inbound envelope.
 *
 *   spec-2.3 hardening v1.0.1:
 *     - F2 (L69 inbound-identity-binding): caller passes peer_id (the
 *       fd's HELLO-bound identity); when peer_id >= 0 we enforce
 *       env->source_node_id == peer_id AND env->source_node_id is a
 *       declared cluster member (cluster_conf_lookup_node).  Pass
 *       peer_id = -1 from pre-handshake / unit-test contexts to skip
 *       binding (range scan still applies).
 *     - F3 (L70 contract-API-NULL-payload): caller passes payload_len
 *       (the actual byte count of the receive buffer).  We reject
 *       env->payload_length > 0 && payload == NULL AND
 *       env->payload_length != payload_len.
 *
 *   8-step verification path:
 *     1. magic == PGRAC_IC_ENVELOPE_MAGIC
 *     2. version == PGRAC_IC_ENVELOPE_VERSION_V1
 *     3. source_node_id != PGRAC_IC_BROADCAST (sender must be concrete);
 *        F2 binding (== peer_id when known + declared member)
 *     4. dest_node_id == self_node_id OR == PGRAC_IC_BROADCAST
 *     5. payload_length <= PGRAC_IC_PAYLOAD_MAX
 *        F3 contract: !(payload_length>0 && payload==NULL); payload_length == payload_len
 *     6. payload_crc32c matches recomputed CRC
 *     7. (spec-2.4) epoch enforce -- field-but-no-enforce in spec-2.3
 *     8. (spec-2.4) Lamport SCN observe -- field-but-no-observe in spec-2.3
 *
 *   self_node_id is passed as a parameter (rather than read from
 *   cluster_node_id global) to keep this function unit-testable in
 *   isolation and to avoid pulling cluster_guc.h dependencies into
 *   this small module.
 *
 *   Returns true if all checks pass; false otherwise.  Caller is
 *   expected to handle false per spec-2.3 §3.5b inbound rule:
 *   peer-level failure (close peer + LOG/WARNING + metric);
 *   NEVER ereport ERROR LMON.
 */
extern bool cluster_ic_envelope_verify(const ClusterICEnvelope *env, const void *payload,
									   uint32 payload_len, uint32 self_node_id, int32 peer_id);

/*
 * spec-2.4 §2.7 Q2 修订 -- stateful Lamport SCN observe API.
 *
 *   cluster_ic_envelope_observe_scn -- advance local SCN per L21
 *     `>=` boundary using env->scn.  CONTRACT: caller MUST call
 *     verify() and receive true return BEFORE calling this.
 *     Calling on a not-yet-verified envelope is a contract violation
 *     (forged / spoofed / stale frames could spoof SCN advance).
 *
 *     Returns true iff local SCN actually advanced;false on no-op
 *     (env.scn == 0 or <= current_scn).  Bumps per-peer
 *     lamport_observe_advance_count metric on advance.
 *
 *   cluster_ic_envelope_accept_and_observe -- LMON-facing wrapper.
 *     Calls verify() then (if pass) observe_scn().  Returns true
 *     iff verify passed.  Production callers (tier1 recv heartbeat
 *     drain + spec-2.4 chunked dispatch) use this;mock / dry-run
 *     / unit-test paths call bare verify() to avoid SCN pollution.
 */
extern bool cluster_ic_envelope_observe_scn(const ClusterICEnvelope *env, int32 source_node_id);
extern bool cluster_ic_envelope_accept_and_observe(const ClusterICEnvelope *env,
												   const void *payload, uint32 payload_len,
												   uint32 self_node_id, int32 peer_id);

/*
 * cluster_ic_envelope_compute_crc -- compute CRC32C over envelope-
 *   excluding-crc + payload.  Coverage is exactly env[0..32] (the
 *   bytes preceding payload_crc32c at offset 32) followed by
 *   payload[0..env->payload_length].
 *
 *   Empty payload (env->payload_length == 0) still produces nonzero
 *   CRC because env[0..32] has multi-byte content (per spec-2.3 §3.3
 *   + Q3 boundary clarification).
 *
 *   Exposed for unit tests; build/verify use internally.
 */
extern uint32 cluster_ic_envelope_compute_crc(const ClusterICEnvelope *env, const void *payload);

#endif /* !FRONTEND */

#endif /* CLUSTER_IC_ENVELOPE_H */
