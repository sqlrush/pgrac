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
	PGRAC_IC_MSG_SINVAL = 7,		/* PGRAC: spec-2.38 D1 真激活 — SI Broadcaster
									 * skeleton (was spec-2.18 reserved).
									 * SinvalBroadcastHeader 24B + variable-length
									 * SharedInvalidationMessage[16B] tail;
									 * producer mask = CLUSTER_IC_PRODUCER_SINVAL_FANOUT
									 * (HC139 — LMON-mediated fanout; backend
									 * 不可 bypass outbound queue) */
	PGRAC_IC_MSG_FENCE_NOTIFY = 8,	/* spec-2.28 reserved */
	PGRAC_IC_MSG_RECONFIG = 9,		/* spec-2.29 reserved */
	/* 11 is already occupied by PGRAC_IC_MSG_CSSD_HEARTBEAT macro in
	 * cluster_cssd.h:107 (spec-2.5 CSSD); skip 10/11 to avoid silent
	 * wire-ABI collision (spec-2.32 v0.2 F1 PG-fact discovery). */
	PGRAC_IC_MSG_GCS_REQUEST = 12, /* PGRAC: spec-2.32 D1 — Cache Fusion GCS request wire */
	PGRAC_IC_MSG_GCS_REPLY = 13,   /* PGRAC: spec-2.32 D1 — Cache Fusion GCS reply wire */
	PGRAC_IC_MSG_GCS_BLOCK_REQUEST
	= 14, /* PGRAC: spec-2.33 D1 — Cache Fusion block ship request wire */
	PGRAC_IC_MSG_GCS_BLOCK_REPLY
	= 15, /* PGRAC: spec-2.33 D1 — Cache Fusion block ship reply wire (8KB payload) */
	PGRAC_IC_MSG_GCS_BLOCK_FORWARD
	= 16, /* PGRAC: spec-2.35 D1 — Cache Fusion 2-way master→holder forward wire (64B) */
	PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE
	= 17, /* PGRAC: spec-2.36 D1 — CF 3-way master→S/X holder invalidate request (64B) */
	PGRAC_IC_MSG_GCS_BLOCK_INVALIDATE_ACK
	= 18, /* PGRAC: spec-2.36 D1 — CF 3-way holder→master invalidate ack (64B);
		   * MUST be a distinct msg_type from INVALIDATE — request+ack are both
		   * 64B fixed, cannot demux by payload length (codereview F1 P0). */
	PGRAC_IC_MSG_SINVAL_ACK = 19, /* PGRAC: spec-2.39 D4 — SI Broadcaster peer_enqueued ACK envelope
		   * (SinvalAckHeader 24B fixed;no tail).  Producer mask = LMON
		   * (L172 family — tier1 fd LMON-exclusive-ownership).  Sent by
		   * LMON drain of ClusterSinvalAckOutbound (single-peer fanout,
		   * not broadcast). */
	PGRAC_IC_MSG_TT_STATUS_HINT = 20 /* PGRAC: spec-3.2 D1 — cross-node Undo TT
		   * status hint(ClusterTTStatusHintMsg 32B fixed payload:8B header
		   * + 24B embedded ClusterTTStatusKey;no tail).  Producer mask = LMON
		   * (L172 family).  Sent by LMON drain of ClusterTTStatusHintOutbound;
		   * fire-and-forget(无 ack);receiver install_local to spec-3.1
		   * overlay using sender_node_id as origin.  spec-3.2 v1.0 FROZEN. */
	,
	PGRAC_IC_MSG_GCS_BLOCK_REDECLARE = 21 /* PGRAC: spec-4.7 D2 — survivor →
		   * (remastered) master block re-declare after reconfiguration
		   * (GcsBlockRedeclarePayload 64B fixed;  fire-and-forget announce of a
		   * locally-held S/X buffer + its page_lsn so the new master can rebuild
		   * the minimal block-resource view — D3).  Producer mask = BUFFER
		   * clients + LMON (the P5 chunked scan runs in the LMON reconfig tick).
		   * recv-time envelope version reject (P1#5) is centralized in
		   * cluster_ic_envelope.c. */
	,
	PGRAC_IC_MSG_HW_ALLOC = 22,		  /* PGRAC: spec-5.7a D1 — HW relation-extend block-range
		   * request (HwAllocRequest fixed payload).  A backend holding HW(X) sends it
		   * to the resid's GES master; the master advances the authority HWM and
		   * flushes an HW_RESERVE record (HW_SEED on first sight) before replying. */
	PGRAC_IC_MSG_HW_ALLOC_REPLY = 23, /* PGRAC: spec-5.7a D1 — HW block-range reply
		   * (HwAllocReply: first_block + granted + status), master -> the originating
		   * backend, correlated by request_id. */
	PGRAC_IC_MSG_KO_FLUSH = 24,		  /* PGRAC: spec-5.7 D6 — KO object-reuse flush request
		   * (KoFlushHeader: relfilenode + fork + first_block + batch_id).  The dropping
		   * node fanouts it to every alive peer before physically removing/truncating the
		   * relfilenode; the peer drops the buffers and replies KO_FLUSH_ACK. */
	PGRAC_IC_MSG_KO_FLUSH_ACK = 25,	  /* PGRAC: spec-5.7 D6 — KO apply-after-drop ACK
		   * (KoFlushAckHeader: batch_id + acker_node + status), peer -> the dropping node,
		   * sent ONLY after the peer has really dropped the relfilenode's buffers. */
	PGRAC_IC_MSG_CLEAN_LEAVE_ANNOUNCE = 26,	  /* PGRAC: spec-5.13 D8 — leaving node ->
		   * survivors (ClusterLeaveAnnouncePayload: leaving_node + leave_epoch +
		   * preflight flag).  preflight=1 probes peer clean_leave_enabled; preflight=0
		   * is the real "I am leaving" announce that enters survivors into leave-aware
		   * reconfig.  Consumed in the membership layer (NOT gated by clean_leave_enabled
		   * GUC) so a disabled survivor still replies LEAVE_DRAIN_NAK rather than going
		   * silent (§3.4 mixed-mode). */
	PGRAC_IC_MSG_LEAVE_DRAIN_ACK = 27,		  /* PGRAC: spec-5.13 D8 — survivor -> leaving node
		   * (ClusterLeaveAckPayload, nak=0): "dropped all refs to the leaving node +
		   * accepted remaster handoff + ready-to-commit".  PRE-epoch readiness ACK
		   * (does NOT wait for / include cache invalidate — §3.1 F1 non-cycle). */
	PGRAC_IC_MSG_LEAVE_DRAIN_NAK = 28,		  /* PGRAC: spec-5.13 D8 — survivor -> leaving node
		   * (ClusterLeaveAckPayload, nak=1): refuse the clean leave (peer disabled /
		   * not in quorum).  Leaving node CLUSTER_LEAVE_ABORTED (clean abort, no
		   * escalate, no epoch bump) on any NAK (§3.4). */
	PGRAC_IC_MSG_LEAVE_COMMIT_READY = 29,	  /* PGRAC: spec-5.13 D6 — leaving node ->
		   * survivor coordinator (ClusterLeaveAnnouncePayload, preflight=0): "I have
		   * drained + every survivor acked; bump the leave epoch now".  The leaving
		   * node self-drives the drain but the survivor coordinator owns the epoch
		   * bump (Q6-A); this is the readiness handoff that triggers the coordinator's
		   * two-phase commit.  Idempotent (re-sent each tick until the CLEAN_LEAVE
		   * commit is observed; the coordinator ignores it once clean_departed). */
	PGRAC_IC_MSG_LEAVE_COMMITTED = 30,		  /* PGRAC: spec-5.13 Hardening v1.0.1 (P1-V0.7) —
		   * survivor coordinator -> leaving node (ClusterLeaveAnnouncePayload, preflight=0):
		   * "the COMMITTED marker is majority-durable; the durable truth-source exists,
		   * you may exit".  Gates the leaving node's BARRIER_WAIT -> COMMITTED transition
		   * so it never departs before the marker is durable (§2.5 exit gate); re-sent
		   * each tick while the leaver is alive (idempotent; best-effort delivery). */
	PGRAC_IC_MSG_NODE_REMOVE_ANNOUNCE = 31,	  /* PGRAC: spec-5.18 D10 — removal coordinator ->
		   * survivors (ClusterNodeRemoveAnnouncePayload: coordinator + target + remove_epoch +
		   * removal_event_id).  Survivors drop their refs to the removed node + reply
		   * REMOVE_CLEANUP_ACK. */
	PGRAC_IC_MSG_REMOVE_CLEANUP_ACK = 32,	  /* PGRAC: spec-5.18 D10 — survivor -> removal
		   * coordinator (ClusterNodeRemoveCleanupAckPayload): "I dropped all refs to the removed
		   * node + accepted the permanent remaster"; sets the survivor's bit in the coordinator's
		   * cleanup ACK barrier. */
	PGRAC_IC_MSG_BACKUP_REQUEST = 33,		  /* PGRAC: spec-6.5 D1/D4 — backup coordinator ->
		   * peers (ClusterBackupWireRequest): START / STOP / ABORT / RESTORE_POINT request.
		   * LMON-mediated; peer LMON executes the local native backup/restore-point leg and
		   * replies with BACKUP_ACK. */
	PGRAC_IC_MSG_BACKUP_ACK = 34,			  /* PGRAC: spec-6.5 D1/D4 — peer -> backup
			   * coordinator (ClusterBackupWireAck): local thread REDO/checkpoint/stop-cut
			   * metadata or fail-closed NAK reason. */
	PGRAC_IC_MSG_SMART_FUSION_DURABLE = 35,	  /* PGRAC: spec-6.2 D8 — origin durable-LSN
			   * gossip for Smart Fusion dependency release. */
	PGRAC_IC_MSG_UNDO_HORIZON = 36,			  /* PGRAC: spec-5.22e D5-2 — per-peer undo
			   * retention horizon report (LMON-only producer; p2p, never
			   * broadcast; capability-gated on UNDO_HORIZON_V1 so an old
			   * peer never sees an unregistered msg_type). */
	PGRAC_IC_MSG_PEER_CAPS_REPLY = 37,		  /* PGRAC: spec-2.2 additive amendment
			   * (spec-5.22e D5 prereq) — acceptor -> dialer capability
			   * reply carrying the acceptor's own standard 64-byte HELLO
			   * as payload (LMON-only producer; p2p, never broadcast;
			   * capability-gated on the dialer's CAPS_REPLY_V1 HELLO bit
			   * so an old dialer never sees an unregistered msg_type). */
	PGRAC_IC_MSG_GCS_BLOCK_DONE = 38,		  /* PGRAC: GCS-race round-2 RC-F —
			   * requester -> master completion proof for an accepted
			   * terminal block reply; the master verifies full identity
			   * and stamps the dedup entry done (advisory: loss is
			   * absorbed by the pinned TTL backstop). */
	PGRAC_IC_MSG_XID_NATIVE_DISABLE = 39,	  /* PGRAC: GCS-race round-3 P0-1 —
			   * wrap-barrier coordinator -> member: clear your
			   * native-prehistory coverage latch (one-way) and ACK; the
			   * NATIVE_RAW_REUSED authority stamp is already durable
			   * (LMON-only producer; p2p per alive member;
			   * capability-gated on XID_NATIVE_DISABLE_V1). */
	PGRAC_IC_MSG_XID_NATIVE_DISABLE_ACK = 40, /* PGRAC: GCS-race round-3 P0-1 —
			   * member -> coordinator: my latch is off; sets the member's
			   * bit in the barrier ack bitmap (LMON-only producer; p2p,
			   * never broadcast). */
	/* PCM-X conversion messages use one stable type per exact payload/phase.
	 * All are point-to-point DATA-plane frames. */
	PGRAC_IC_MSG_PCM_X_ENQUEUE = 41,
	PGRAC_IC_MSG_PCM_X_ADMIT_ACK = 42,
	PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM = 43,
	PGRAC_IC_MSG_PCM_X_ADMIT_CONFIRM_ACK = 44,
	PGRAC_IC_MSG_PCM_X_BLOCKER_SET_BEGIN = 45,
	PGRAC_IC_MSG_PCM_X_BLOCKER_SET_EDGE = 46,
	PGRAC_IC_MSG_PCM_X_BLOCKER_SET_COMMIT = 47,
	/* PcmXPhasePayload [88,96) generation: zero = master->holder PROBE;
	 * nonzero = generation-exact master->holder BLOCKER_SET ACK. */
	PGRAC_IC_MSG_PCM_X_BLOCKER_SET_ACK = 48,
	PGRAC_IC_MSG_PCM_X_REVOKE = 49,
	PGRAC_IC_MSG_PCM_X_IMAGE_READY = 50,
	PGRAC_IC_MSG_PCM_X_PREPARE_GRANT = 51,
	PGRAC_IC_MSG_PCM_X_INSTALL_READY = 52,
	PGRAC_IC_MSG_PCM_X_COMMIT_X = 53,
	PGRAC_IC_MSG_PCM_X_FINAL_ACK = 54,
	PGRAC_IC_MSG_PCM_X_FINAL_COMMIT_ACK = 55,
	PGRAC_IC_MSG_PCM_X_FINAL_CONFIRM = 56,
	PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL = 57,
	PGRAC_IC_MSG_PCM_X_PREHANDLE_CANCEL_ACK = 58,
	PGRAC_IC_MSG_PCM_X_CANCEL = 59,
	PGRAC_IC_MSG_PCM_X_CANCEL_ACK = 60,
	PGRAC_IC_MSG_PCM_X_DRAIN_POLL = 61,
	PGRAC_IC_MSG_PCM_X_DRAIN_ACK = 62,
	PGRAC_IC_MSG_PCM_X_RETIRE_UP_TO = 63,
	PGRAC_IC_MSG_PCM_X_RETIRE_ACK = 64
	/* values 65..255 available for future sub-spec; never reuse 0..64 */
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
/*
 * spec-2.4 hardening v1.0.1 F4 (L75 verify-tri-state-return):
 * verify reject has two distinct semantics that need different
 * caller actions:
 *   - DROP_NO_CLOSE: stale epoch (peer is alive, just sending pre-
 *     reconfig in-flight frames or replay).  LOG + counter + drop
 *     frame; KEEP peer connected.
 *   - PEER_FAILURE:  bad magic/version/source/dest/CRC/payload contract.
 *     Peer is hostile or wire-corrupt.  Caller MUST close peer.
 * Single bool return mixed these together → tier1 caller treated
 * stale epoch as peer failure → close peer (semantic mismatch with
 * envelope.c's "peer NOT closed" intent).  Three-state fixes.
 */
typedef enum ClusterICEnvelopeVerifyResult {
	CLUSTER_IC_ENVELOPE_OK = 0,		   /* dispatch */
	CLUSTER_IC_ENVELOPE_DROP_NO_CLOSE, /* stale epoch only;peer NOT closed */
	CLUSTER_IC_ENVELOPE_PEER_FAILURE,  /* hard verify fail;close peer */
} ClusterICEnvelopeVerifyResult;

extern ClusterICEnvelopeVerifyResult cluster_ic_envelope_verify(const ClusterICEnvelope *env,
																const void *payload,
																uint32 payload_len,
																uint32 self_node_id, int32 peer_id);

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

/*
 * spec-2.4 hardening v1.0.1 F4: tri-state return.  Caller switches:
 *   OK              -> dispatch
 *   DROP_NO_CLOSE   -> drop frame, keep peer connected
 *   PEER_FAILURE    -> close peer
 */
extern ClusterICEnvelopeVerifyResult
cluster_ic_envelope_accept_and_observe(const ClusterICEnvelope *env, const void *payload,
									   uint32 payload_len, uint32 self_node_id, int32 peer_id);

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
