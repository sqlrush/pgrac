/*-------------------------------------------------------------------------
 *
 * cluster_ic.h
 *	  pgrac cluster internal IPC abstraction layer (Stage 0.18 stub).
 *
 *	  This header declares the IPC contract that every pgrac cross-node
 *	  subsystem (PCM, GES, Cache Fusion, SCN, Sinval, Heartbeat,
 *	  Recovery, TT lookup) will call into starting in Stage 2.  At
 *	  stage 0.18 the implementation is a stub: target == self is a
 *	  no-op success, target != self ereports
 *	  ERRCODE_FEATURE_NOT_SUPPORTED.  Stage 2 swaps in a TCP-backed
 *	  vtable, and Stage 6.1 adds the RDMA-capable mux and tier
 *	  vtables; the API surface declared here stays unchanged across
 *	  that evolution.
 *
 *	  Two layers, both exported:
 *
 *	    Low-level byte stream    cluster_ic_send_bytes / recv_bytes
 *	    High-level protocol      cluster_msg_send / cluster_msg_recv
 *	                             cluster_rpc_call (sync request-reply)
 *
 *	  99% of subsystems should use the high-level API.  The byte stream
 *	  is reserved for performance-critical paths that need transport-
 *	  specific handling such as RDMA SEND-with-SGE block shipping.
 *
 *	  Wire format is the 36-byte ClusterICEnvelope followed by an opaque
 *	  payload; cluster_ic_envelope.c owns the ABI StaticAssertDecls.
 *
 *	  See docs/cluster-ic-design.md for the full design rationale and
 *	  Stage evolution path; specs/spec-0.18-ic-framework.md for the
 *	  stage-0 scope and exit criteria.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ic.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  The cluster_ic_* / cluster_msg_* / cluster_rpc_* symbols are
 *	  available only when configured with --enable-cluster
 *	  (USE_PGRAC_CLUSTER defined); call sites must be guarded.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_IC_H
#define CLUSTER_IC_H

#include "fmgr.h"
#include "port/pg_crc32c.h"


/*
 * ClusterICTier -- the four interconnect tiers defined in
 *	interconnect-tier-strategy.md.  The cluster.interconnect_tier GUC
 *	maps onto this enum; cluster_ic_init picks the corresponding
 *	vtable.  CLUSTER_IC_TIER_STUB and CLUSTER_IC_TIER_MOCK are
 *	supported; tier1 lands later, tier2/tier3 later still.
 */
typedef enum ClusterICTier {
	CLUSTER_IC_TIER_STUB = 0,
	CLUSTER_IC_TIER_1 = 1,
	CLUSTER_IC_TIER_2 = 2,
	CLUSTER_IC_TIER_3 = 3,
	CLUSTER_IC_TIER_MOCK = 4
} ClusterICTier;


/*
 * spec-2.3 D3: wire format unified into 36-byte ClusterICEnvelope
 * (cluster_ic_envelope.h).  spec-2.2 ClusterMsgHeader (24 bytes,
 * PGRAC_IC_MAGIC 0x47435249, PGRAC_IC_PROTOCOL_VERSION_V1) deleted;
 * spec-2.2 §3.9 hard scope guard at the byte-stream layer
 * (cluster_ic_send_bytes B_LMON check) deleted -- replaced by
 * cluster_ic_router's producer_mask validation per msg_type
 * registration.  Lower-level send_bytes/recv_bytes vtable surface
 * preserved; only the higher-level cluster_msg_send/recv layer
 * removed.
 */
#include "cluster/cluster_ic_envelope.h"


/*
 * The cluster IC implementation surface is only present when configured
 * with --enable-cluster.  Disable-cluster builds must not link any
 * cluster_ic_* / cluster_msg_* / cluster_rpc_* symbol; cluster/Makefile
 * enforces this by excluding cluster_ic.o from disable-mode OBJS.
 *
 * spec-0.3 symbol contract: nm /path/to/postgres | grep -E
 * '(cluster_ic|cluster_msg|cluster_rpc|ClusterICOps)' must be empty in
 * disable-cluster binaries.
 */
#ifdef USE_PGRAC_CLUSTER

/*
 * ClusterICOps -- vtable of the active interconnect tier.
 *
 *	Stage 0.18 ships exactly one vtable instance, ClusterICOps_Stub.
 *	Stage 2 adds ClusterICOps_Tier1 (TCP); Stage 6.1 adds the
 *	ClusterICOps_Mux plus Tier2/Tier3 RDMA vtables.  The
 *	active tier is selected at postmaster startup based on the
 *	cluster.interconnect_tier GUC and stored in ClusterICOps_Active.
 *
 *	Function-pointer fields must remain non-NULL for any vtable shipped;
 *	NULL is never a valid value at runtime (cluster_ic_init asserts).
 */
/*
 * spec-2.2 D2 -- vtable contract (revised post-Sprint A Step 1-5
 * codex review):
 *
 *   send_bytes:
 *     Returns true on hand-off success, false on hard error.
 *
 *   recv_bytes (STRICT semantics, post-codex-review):
 *     - Returns true with *out_received > 0 on success (got data).
 *     - Returns true with *out_received == 0 on "no data ready"
 *       (mock: empty inbound queue; tier1: kernel EAGAIN/EWOULDBLOCK
 *       race after WaitEventSet).  This is NOT an error -- the
 *       caller's wait loop should re-poll later.
 *     - Returns false on HARD ERROR ONLY (mock: never; tier1:
 *       ECONNRESET / EPIPE / unrecoverable socket state).
 *     - Pre-codex-review (Sprint A Step 1-5 commit f4797e6001), mock
 *       returned false for empty queue, conflating "no data" with
 *       "hard error" -- this would silently swallow tier1 ECONNRESET
 *       once D3 lands.  Strict bool semantics fix that.
 *
 *   peek_sender (NEW per codex review):
 *     - Returns true and sets *out_sender if a chunk is currently
 *       available to recv (mock: queue head non-NULL; tier1: a
 *       per-peer fd is readable AND has unconsumed bytes).
 *     - Returns false if no chunk available right now.
 *     - Pure peek -- MUST NOT mutate any state (no consumed +=,
 *       no queue pop, no socket read).  Used by cluster_ic_recv_exact
 *       to detect sender flip BEFORE consuming bytes from a different
 *       peer (which would pollute the caller's buffer).
 *
 *   tier_init / tier_shutdown:
 *     Called by cluster_ic_init / cluster_ic_shutdown after vtable
 *     bound to ClusterICOps_Active.
 */
/*
 * spec-2.3 hardening v1.0.1 F1 (L68 backpressure-≠-peer-death):
 * three-state send result.  Caller MUST distinguish backpressure
 * (WOULD_BLOCK; outbound buffer holds the tail; the next call after
 * WL_SOCKET_WRITEABLE will drain) from real socket death
 * (HARD_ERROR; close peer + state=DOWN).  Mixing the two in a bool
 * return value caused spec-2.2 v1.0.1 F1's per-peer outbound buffer
 * to be silently bypassed -- LMON closed the peer on the very first
 * EAGAIN, and the tail-drain path was never reached.
 *
 * GCS serve-stall round-5: four-state FRAME-OWNERSHIP contract.
 * WOULD_BLOCK was ambiguous — "transport retained the frame" (partial
 * write / initial-EAGAIN tail queue) and "transport REFUSED the frame"
 * (a previous tail still pending / peer mid-HELLO) both returned it.
 * Producers that trusted the documented "retained" reading silently
 * lost every frame sent while an older tail was backpressured (a lost
 * GCS reply = the requester burns its full reply-wait + retransmit
 * budget = the 33-54s S3 stall wall);  ring drains that assumed the
 * "refused" reading resubmitted frames the transport HAD retained
 * (duplicate frames on the per-peer stream).  The contract is now:
 *
 *   DONE          full frame on the wire.
 *   WOULD_BLOCK   frame ADMITTED: the transport owns a private copy
 *                 (partial-write tail or per-peer outbound FIFO) and
 *                 will finish it on WL_SOCKET_WRITEABLE.  The caller
 *                 must NEVER resubmit the frame.
 *   NOT_ADMITTED  frame REFUSED: the transport took no copy (peer not
 *                 CONNECTED yet, or the bounded per-peer FIFO is
 *                 full).  The caller retains ownership: keep it in
 *                 the upper-layer queue / retry later / count the
 *                 refusal.  Never treat as peer death.
 *   HARD_ERROR    socket dead;caller MUST close the peer.
 */
typedef enum ClusterICSendResult {
	CLUSTER_IC_SEND_DONE = 0,	  /* full frame sent;counter advance */
	CLUSTER_IC_SEND_WOULD_BLOCK,  /* frame admitted;transport owns a copy */
	CLUSTER_IC_SEND_HARD_ERROR,	  /* socket dead;caller MUST close peer */
	CLUSTER_IC_SEND_NOT_ADMITTED, /* frame refused;caller retains ownership */
} ClusterICSendResult;

typedef struct ClusterICOps {
	ClusterICSendResult (*send_bytes)(int32 target_node_id, const void *buf, size_t len);
	bool (*recv_bytes)(int32 *out_sender_node_id, void *buf, size_t bufsize,
					   size_t *out_received_len);
	bool (*peek_sender)(int32 *out_sender_node_id);
	void (*tier_init)(void);
	void (*tier_shutdown)(void);
	const char *tier_name;
} ClusterICOps;

extern const ClusterICOps ClusterICOps_Stub;
extern const ClusterICOps ClusterICOps_Mock;

/*
 * spec-2.2 D1 -- Tier1 (TCP) vtable extern.  Implementation in
 * cluster_ic_tier1.c (NEW; spec-2.2 D3).  ClusterICOps_Active is
 * bound to this when cluster.interconnect_tier = tier1 AND
 * cluster_enabled = true (per v1.0.2 D-I1 double gate;
 * spec-2.2 §3.7).
 */
extern const ClusterICOps ClusterICOps_Tier1;
extern const ClusterICOps ClusterICOps_Tier2;
extern const ClusterICOps ClusterICOps_Tier3;
extern const ClusterICOps ClusterICOps_Mux;

extern const ClusterICOps *ClusterICOps_Active;


/*
 * spec-2.2 D1 -- HELLO handshake message exchanged on every newly
 * established TCP connection.
 *
 * Protocol shape (asymmetric; spec-2.2 §2.4 + Hardening v1.0.1 F5):
 *   - Active connect side sends HELLO and considers itself CONNECTED
 *     once the 64 bytes are fully written.  No HELLO_ACK frame.
 *   - Passive accept side verifies HELLO (cluster_name +
 *     source_node_id + version match per §3.10); on success flips
 *     peer state to CONNECTED, on rejection silently closes the
 *     socket.  Active side detects rejection via the next heartbeat
 *     send/recv error -> close + DOWN -> reconnect.
 *
 * Fixed 64-byte ABI for cross-version safety
 * (StaticAssertDecl in cluster_ic_tier1.c).
 *
 * Per spec-2.2 §2.4, HELLO carries:
 *   magic + hello_version (allows future bump without breaking pre-v1
 *   peers; current peers must reject mismatches per §3.10),
 *   envelope_version (separate from hello_version because envelope is
 *   spec-2.0 §4 frozen v1; HELLO can evolve independently),
 *   source_node_id (sender's cluster.node_id; verified against
 *   pgrac.conf [node.N] declaration),
 *   cluster_name (verified against ClusterConfShmem->cluster_name; the
 *   primary defense against accidentally connecting to wrong cluster).
 *
 * Failure to verify any field => connection-level rejection per §3.10
 * (close socket + peer_state = rejected; NEVER FATAL the postmaster).
 */
#define PGRAC_IC_HELLO_MAGIC ((uint32)0x4F4C4C48) /* "HLLO" LE */
#define PGRAC_IC_HELLO_VERSION_V1 ((uint16)1)

/*
 * PGRAC: spec-7.2 D2 — interconnect plane.  CONTROL is the historic
 * LMON-owned connection (heartbeat / membership / GES / everything
 * registered before spec-7.2);  DATA is the LMS-owned connection the
 * GCS block family migrates to.  Zero MUST stay CONTROL: a pre-7.2
 * HELLO carries all-zero pad bytes and must keep parsing as a
 * CONTROL-plane peer.
 */
typedef enum ClusterICPlane {
	CLUSTER_IC_PLANE_CONTROL = 0,
	CLUSTER_IC_PLANE_DATA = 1
} ClusterICPlane;

#define CLUSTER_IC_PLANE_N 2
/*
 * spec-2.3 D3: PGRAC_IC_ENVELOPE_VERSION_V1 is now defined in
 * cluster_ic_envelope.h as ((uint8)1) — the authoritative
 * envelope wire constant.  HELLO carries it in a 2-byte slot
 * (frozen ABI) via implicit promotion to uint16 at use site.
 */
#define PGRAC_IC_HELLO_BYTES 64
#define PGRAC_IC_CLUSTER_NAME_MAX 24
#define PGRAC_IC_HELLO_CAPABILITIES_OFFSET 36
#define PGRAC_IC_HELLO_CAP_SMART_FUSION_REPLY_V2 ((uint32)0x00000001U)
/* PGRAC: spec-5.22d D4-6 — this binary understands the kind-4 dead-owner
 * AUTHORITY verdict request and the version-2 authority-served verdict page.
 * A PROTOCOL capability: advertised unconditionally (unlike the GUC/tier-
 * gated smart-fusion bit above) — whether the serve actually runs is the
 * serve side's runtime GUC gate, which refuses with DENIED and the requester
 * stays fail-closed.  A requester only routes kind 4 to a peer that
 * advertised this bit; without it the authority leg fails closed (the
 * election is NOT re-run against a different node). */
#define PGRAC_IC_HELLO_CAP_UNDO_AUTHORITY_SERVE_V1 ((uint32)0x00000002U)
/* PGRAC: spec-5.22e D5-2 — this binary registers PGRAC_IC_MSG_UNDO_HORIZON
 * and publishes/consumes undo retention horizon reports.  A PROTOCOL
 * capability, advertised unconditionally.  Send-side hard gate: a report is
 * only sent to a peer whose CURRENT connection advertised this bit (an old
 * peer treats the unregistered msg_type as a peer-level failure and closes
 * the connection).  Fold-side: a required MEMBER peer without this bit
 * stalls recycling (NOCAP) — never a fallback to local-horizon recycling
 * (Q3'').  Capability state is connection-bound: cleared on peer close and
 * only reinstated by the next HELLO (Q1' amend). */
#define PGRAC_IC_HELLO_CAP_UNDO_HORIZON_V1 ((uint32)0x00000004U)
/* PGRAC: spec-2.2 additive amendment (spec-5.22e D5 prereq) — META
 * capability: "this binary registers PGRAC_IC_MSG_PEER_CAPS_REPLY and can
 * receive it".  Advertised unconditionally (suppressible only by the
 * test-only cluster.ic_suppress_caps_reply old-binary simulation GUC).
 * The acceptor sends PEER_CAPS_REPLY back to a verified dialer ONLY when
 * the dialer's HELLO carried this bit, so an old binary is never sent a
 * frame whose msg_type it would reject by closing the connection.  The
 * active handshake sequence is unchanged (send HELLO -> CONNECTED, no
 * wait): a missing reply just leaves the dialer's view of the peer's
 * capabilities UNKNOWN, which every consumer treats as fail-closed. */
#define PGRAC_IC_HELLO_CAP_CAPS_REPLY_V1 ((uint32)0x00000008U)
/* PGRAC: GCS-race round-2 review F6 — this binary registers
 * PGRAC_IC_MSG_GCS_BLOCK_DONE, SENDS the completion proof after consuming a
 * terminal reply, and pins its dedup registrations with a wire lifetime
 * hint.  A PROTOCOL capability, advertised unconditionally.  Send-side hard
 * gate: a requester only sends DONE to a peer that advertised this bit (an
 * old peer treats the unregistered msg_type as a peer-level failure and
 * closes the connection); without it the entry simply ages out on its
 * pinned TTL.  Master-side consumption: a request from a peer WITHOUT this
 * bit is a legacy registration (pinned at the protocol-maximum lifetime,
 * never reclaimed early); a peer WITH the bit that sends hint==0 or an
 * over-maximum hint is a protocol violation (counted, denied). */
#define PGRAC_IC_HELLO_CAP_GCS_DONE_V1 ((uint32)0x00000010U)
/* PGRAC: GCS-race round-3 P0-1 — this binary registers
 * PGRAC_IC_MSG_XID_NATIVE_DISABLE(_ACK) and participates in the xid wrap
 * barrier.  A PROTOCOL capability, advertised unconditionally.  Send-side
 * hard gate: the barrier coordinator only sends DISABLE to a peer that
 * advertised this bit; a member without it can never ACK, so the barrier
 * stays incomplete and the allocation gate keeps refusing epoch>=1
 * candidates fail-closed -- the correct posture for a mixed-version
 * cluster near the xid boundary (upgrade the old binary to proceed). */
#define PGRAC_IC_HELLO_CAP_XID_NATIVE_DISABLE_V1 ((uint32)0x00000020U)
/* PGRAC: GCS-race round-4 P0-1 — this binary serializes every shared XID
 * authority mutation through the flock critical section and writes the
 * stamped (RAW_REUSED) magic protocol.  DISTINCT from the barrier bit
 * above: 0x20 only proves the DISABLE/ACK wire is understood; a 0x20-only
 * binary still runs lock-free authority read-modify-writes and can erase
 * a concurrent stamp with its stale header.  The wrap barrier therefore
 * refuses to open the allocation gate until EVERY conf-declared member is
 * connected and advertises THIS bit (declared-but-unreachable could be
 * exactly such a writer mid-boot) -- the stop-the-world-upgrade posture.
 * Post-stamp, an old binary fail-closes on the stamped magic before it
 * can reach any transition write, so the exposure window is only ever
 * pre-gate-open, where epoch>=1 xids cannot exist and a re-latch is
 * harmless; the LMON tick's settle re-verify + re-assert repairs any
 * erased stamp until the gate's admission holds. */
#define PGRAC_IC_HELLO_CAP_XID_AUTHORITY_FLOCK_V2 ((uint32)0x00000040U)
/* PGRAC: ownership-generation wave (user ruling ②) — this binary understands
 * INVALIDATE-ACK status RETRYABLE_BUSY(5): a holder that cannot invalidate
 * RIGHT NOW (in-flight grant marked GRANT_PENDING, or a pinned copy) replies
 * BUSY instead of parking silently, and this master aborts the invalidate
 * round immediately (no acked_bm credit, no holder clear, no watermark
 * advance, no X grant), clears pending_x, releases the node-wide broadcast
 * slot and retries with a NEW round identity after a short backoff.  Kills
 * the timeout-mediated progress loop (a reader's S acquire waits on
 * pending_x while the writer waits on an ACK the reader's GRANT_PENDING
 * parks).  A PROTOCOL capability, advertised unconditionally.  Send-side
 * hard gate: a holder replies BUSY only to a master whose CURRENT connection
 * advertised this bit — an old master's ACK handler drops status>2 as a
 * stale reply and would still burn its full timeout; the holder falls back
 * to the round-5 park (old behavior) so mixed-version degrades to exactly
 * the pre-BUSY protocol.  Timeout stays the backstop for packet loss / dead
 * nodes. */
#define PGRAC_IC_HELLO_CAP_GCS_INVAL_BUSY_V1 ((uint32)0x00000080U)
/*
 * PGRAC: spec-7.2 D2 — plane + connection-epoch ride the documented-zero
 * pad region (capabilities precedent: occupy pad bytes, do not resize V1).
 * A pre-7.2 sender leaves them zero => plane CONTROL, conn_epoch 0.
 * Offsets 41/42 are reserved for spec-7.3 (worker_id / n_workers) and
 * stay zero in 7.2;  offset 43 pads the group;  52-63 remain reserved.
 */
#define PGRAC_IC_HELLO_PLANE_OFFSET 40
#define PGRAC_IC_HELLO_WORKER_ID_OFFSET 41 /* spec-7.3 reserved, zero in 7.2 */
#define PGRAC_IC_HELLO_N_WORKERS_OFFSET 42 /* spec-7.3 reserved, zero in 7.2 */
#define PGRAC_IC_HELLO_CONN_EPOCH_OFFSET 44

typedef struct ClusterICHelloMsg {
	uint32 magic;								  /* PGRAC_IC_HELLO_MAGIC */
	uint16 hello_version;						  /* PGRAC_IC_HELLO_VERSION_V1 */
	uint16 envelope_version;					  /* PGRAC_IC_ENVELOPE_VERSION_V1 */
	int32 source_node_id;						  /* sender's cluster.node_id */
	char cluster_name[PGRAC_IC_CLUSTER_NAME_MAX]; /* NUL-terminated; truncated */
	uint8 _pad[28];								  /* pad to 64B fixed ABI */
} ClusterICHelloMsg;

static inline uint32
cluster_ic_hello_capabilities(const ClusterICHelloMsg *msg)
{
	const uint8 *p;

	if (msg == NULL)
		return 0;
	p = msg->_pad;
	return ((uint32)p[0]) | ((uint32)p[1] << 8) | ((uint32)p[2] << 16) | ((uint32)p[3] << 24);
}

/* PGRAC: spec-7.2 D2 — plane byte accessor (_pad offset 40 - 36 = 4). */
static inline ClusterICPlane
cluster_ic_hello_plane(const ClusterICHelloMsg *msg)
{
	if (msg == NULL)
		return CLUSTER_IC_PLANE_CONTROL;
	return (ClusterICPlane)
		msg->_pad[PGRAC_IC_HELLO_PLANE_OFFSET - PGRAC_IC_HELLO_CAPABILITIES_OFFSET];
}

/* PGRAC: spec-7.2 D2 — connection epoch accessor (_pad offset 44 - 36 = 8;
 * LE uint64).  Zero = pre-7.2 sender (or CONTROL plane, which does not
 * enforce it);  the DATA-plane verify path rejects zero fail-closed. */
static inline uint64
cluster_ic_hello_conn_epoch(const ClusterICHelloMsg *msg)
{
	const uint8 *p;
	uint64 v = 0;
	int i;

	if (msg == NULL)
		return 0;
	p = msg->_pad + (PGRAC_IC_HELLO_CONN_EPOCH_OFFSET - PGRAC_IC_HELLO_CAPABILITIES_OFFSET);
	for (i = 7; i >= 0; i--)
		v = (v << 8) | (uint64)p[i];
	return v;
}

/* PGRAC: spec-7.3 D3 — DATA-plane worker_id / n_workers accessors (offsets
 * 41/42).  Zero for a pre-7.3 sender or a CONTROL-plane HELLO. */
static inline uint8
cluster_ic_hello_worker_id(const ClusterICHelloMsg *msg)
{
	if (msg == NULL)
		return 0;
	return msg->_pad[PGRAC_IC_HELLO_WORKER_ID_OFFSET - PGRAC_IC_HELLO_CAPABILITIES_OFFSET];
}

static inline uint8
cluster_ic_hello_n_workers(const ClusterICHelloMsg *msg)
{
	if (msg == NULL)
		return 0;
	return msg->_pad[PGRAC_IC_HELLO_N_WORKERS_OFFSET - PGRAC_IC_HELLO_CAPABILITIES_OFFSET];
}


/*
 * spec-2.2 D2 (post-codex review) -- HELLO wire encode/decode helpers.
 *
 * DO NOT send/recv ClusterICHelloMsg directly across the TCP socket.
 * Compiler struct padding, alignment, and byte order may differ
 * between sender and receiver -- and uninitialized stack pad bytes
 * can leak sensitive memory contents onto the wire.  Always go
 * through cluster_ic_build_hello / cluster_ic_parse_hello which:
 *
 *   - memset the 64-byte buffer to zero (no leaked pad bytes)
 *   - write each field at its frozen wire offset
 *   - serialize multi-byte integers in little-endian (consistent
 *     with the rest of pgrac wire format; see ClusterMsgHeader,
 *     spec-2.0 §4 envelope)
 *   - truncate cluster_name to PGRAC_IC_CLUSTER_NAME_MAX-1 + NUL
 *
 * The wire layout is locked at unit-test level via a fixed byte-vector
 * roundtrip (test_hello_wire_roundtrip).  Any future bump to HELLO
 * MUST go via PGRAC_IC_HELLO_VERSION_V2 (new struct + dispatch on
 * hello_version field), never resize V1 in-place.
 */
extern void cluster_ic_build_hello(uint8 out_buf[PGRAC_IC_HELLO_BYTES], uint16 hello_version,
								   uint16 envelope_version, int32 source_node_id,
								   const char *cluster_name, ClusterICPlane plane,
								   uint64 conn_epoch);
extern bool cluster_ic_parse_hello(const uint8 in_buf[PGRAC_IC_HELLO_BYTES],
								   ClusterICHelloMsg *out_msg);

/*
 * spec-7.3 D3 — write the DATA-plane worker_id / n_workers fields (offsets
 * 41/42) onto an already-built HELLO buffer.  Only the DATA-plane send path
 * calls it;  a CONTROL / pre-7.3 HELLO leaves them zero (byte-identical to
 * spec-7.2).  Kept next to build_hello as the wire-layout authority.
 */
extern void cluster_ic_hello_set_worker_fields(uint8 out_buf[PGRAC_IC_HELLO_BYTES], uint8 worker_id,
											   uint8 n_workers);


/*
 * spec-2.2 D1 -- per-peer state machine state.  Ordering is
 * intentional: numerically ascending = increasing "operational"
 * level.  State transitions are detailed in spec-2.2 §3.4 (readiness)
 * and §3.10 (HELLO failure).
 *
 *   DOWN       -- never connected yet, OR last connect/recv failed,
 *                 OR heartbeat 3x interval missed; reconnect scheduled
 *                 with exponential backoff (1s/2s/4s/8s/max 30s).
 *   CONNECTING -- TCP connect(2) issued (active edge) OR accept(2)
 *                 returned a fresh fd (passive edge); HELLO not yet
 *                 verified.
 *   CONNECTED  -- HELLO verified both ways; heartbeat exchange active.
 *   REJECTED   -- HELLO verification failed (wrong magic / version /
 *                 cluster_name / node_id); peer permanently rejected
 *                 until next reconnect attempt re-tries HELLO.
 *
 * Per spec-2.2 §3.6 boundary invariant, these are TRANSPORT-LEVEL
 * states ONLY.  They do NOT map to cluster membership / quorum /
 * fence state (those land in spec-2.5 / 2.6 / 2.28).
 */
typedef enum ClusterICPeerState {
	CLUSTER_IC_PEER_DOWN = 0,
	CLUSTER_IC_PEER_CONNECTING = 1,
	CLUSTER_IC_PEER_CONNECTED = 2,
	CLUSTER_IC_PEER_REJECTED = 3
} ClusterICPeerState;


/*
 * spec-2.2 D1 -- mesh role decision for the N×(N-1)/2 mesh.
 *
 * Per §2.2 + §3.5 invariant: in any unordered pair {self, peer}, the
 * lower node_id takes the ACTIVE role (initiates connect(2)) and the
 * higher node_id takes the PASSIVE role (accepts on listener).  Race
 * resolution (both sides momentarily ACTIVE due to concurrent connect
 * attempts) closes the connection on the side where mesh_role_for_pair
 * returned PASSIVE for the dup.
 *
 * Pure stateless function -- declared as static inline so unit tests
 * (test_cluster_ic.c) link without pulling in the full Tier1 vtable.
 */
typedef enum ClusterICMeshRole {
	CLUSTER_IC_MESH_ACTIVE = 0,
	CLUSTER_IC_MESH_PASSIVE = 1
} ClusterICMeshRole;

static inline ClusterICMeshRole
cluster_ic_mesh_role_for_pair(int32 self_node_id, int32 peer_node_id)
{
	/* self == peer is a programming error (mesh has no self loop). */
	Assert(self_node_id != peer_node_id);
	return (self_node_id < peer_node_id) ? CLUSTER_IC_MESH_ACTIVE : CLUSTER_IC_MESH_PASSIVE;
}


/*
 * cluster_ic_init -- select the vtable for the configured interconnect
 *	tier and call its tier_init().  Called once at postmaster startup
 *	from cluster_shmem.c::cluster_init_shmem (after PG shmem layout is
 *	in place; cluster_ic at stage 0.18 does not allocate shmem itself,
 *	but Stage 2+ TCP vtable will).
 *
 *	On invalid tier values, ereports ERRCODE_FEATURE_NOT_SUPPORTED with
 *	an errhint pointing to the Stage where each tier lands.
 */
extern void cluster_ic_init(void);

/*
 * cluster_ic_shutdown -- mirror of cluster_ic_init: invoke the active
 *	vtable's tier_shutdown() and clear ClusterICOps_Active.  Called
 *	from cluster_shutdown when wired (stage 0.18 stub: cluster_shutdown
 *	is itself a stub, so this entry exists for forward symmetry).
 */
extern void cluster_ic_shutdown(void);


/* ----------
 * Low-level byte-stream API.
 *
 *	cluster_ic_send_bytes(target, buf, len)
 *	    target == cluster_node_id : stub returns true (no-op success)
 *	    target == -1              : ereport(ERROR, "node_id unconfigured")
 *	    target != self            : ereport(ERROR,
 *	                                ERRCODE_FEATURE_NOT_SUPPORTED,
 *	                                errhint("set interconnect_tier"))
 *
 *	cluster_ic_recv_bytes(...)    : stub returns false (no messages).
 *
 *	The buf arguments are opaque bytes; protocol-aware callers should
 *	use cluster_msg_send / cluster_msg_recv instead.
 * ----------
 */
/*
 * spec-2.3 hardening v1.0.1 F1: send_bytes returns three-state.
 * Caller MUST switch on the result; treating WOULD_BLOCK as failure
 * silently bypasses the partial-IO outbound buffer.
 */
extern ClusterICSendResult cluster_ic_send_bytes(int32 target_node_id, const void *buf, size_t len);

/*
 * spec-2.2 D2 / Q11=A / P2-1 -- recv_exact helper.  Loops over
 * cluster_ic_recv_bytes until exactly bufsize bytes are received from
 * a single peer (or EOF / hard error / sender flip).  Use this for
 * length-prefixed wire-format reads (header / envelope / HELLO).
 * See body comment in cluster_ic.c for full semantics.
 */
extern bool cluster_ic_recv_exact(int32 *out_sender_node_id, void *buf, size_t bufsize,
								  size_t *out_received_len);

extern bool cluster_ic_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize,
								  size_t *out_received_len);


/* ----------
 * High-level protocol API -- DELETED in spec-2.3 D3.
 *
 *	spec-2.2 cluster_msg_send / cluster_msg_recv (24-byte
 *	ClusterMsgHeader + CRC + per-target seq_no) replaced by
 *	cluster_ic_send_envelope / cluster_ic_dispatch_envelope in
 *	cluster_ic_router.h.  New layer adds:
 *	  - 36-byte ClusterICEnvelope (spec-2.0 §4 frozen ABI)
 *	  - msg_type registration table with allowed_producer_mask
 *	    + handler dispatch (replaces spec-2.2 §3.9 LMON-only
 *	    hard-coded scope guard)
 *	  - PG_TRY/PG_CATCH wrap on dispatch (LMON main loop韧性)
 *
 *	cluster_rpc_call (sync request-reply) deferred to spec-2.13
 *	(GES); not in spec-2.3 scope.
 * ----------
 */

#endif /* USE_PGRAC_CLUSTER */


/* ----------
 * Mock-tier SRF surface.  These functions are unconditional symbols
 * (referenced unconditionally from pg_proc.dat); the bodies are
 * #ifdef USE_PGRAC_CLUSTER guarded and return errors / empty in
 * disable-cluster builds.
 *
 *	cluster_ic_mock_inject(from int4, payload bytea) RETURNS void
 *	    Push (from_node, payload) into this backend's mock_inbound_queue.
 *	    Subsequent cluster_ic_recv_bytes / cluster_msg_recv calls in
 *	    the same backend will dequeue this entry.  ERRORs unless
 *	    cluster.interconnect_tier = 'mock'.
 *
 *	cluster_ic_mock_drain_outbound(target int4)
 *	    RETURNS SETOF (sender int4, payload bytea)
 *	    Drain all queued outbound messages whose target is `target`.
 *	    Returns one row per message (FIFO); clears that target's
 *	    outbound queue.  ERRORs unless tier='mock' or target out of [0, 127].
 *
 *	cluster_ic_mock_clear_all() RETURNS void
 *	    Reset all mock queues (inbound + every target's outbound).
 *
 *	cluster_ic_mock_recv_test()
 *	    RETURNS SETOF (sender int4, payload bytea)
 *	    Test-only wrapper: invoke cluster_ic_recv_bytes once and emit
 *	    a single row if a message was dequeued, zero rows otherwise.
 *	    Lets TAP tests verify the recv path without a custom backend.
 * ---------- */
extern Datum cluster_ic_mock_inject(PG_FUNCTION_ARGS);
extern Datum cluster_ic_mock_drain_outbound(PG_FUNCTION_ARGS);
extern Datum cluster_ic_mock_clear_all(PG_FUNCTION_ARGS);
extern Datum cluster_ic_mock_recv_test(PG_FUNCTION_ARGS);

#endif /* CLUSTER_IC_H */
