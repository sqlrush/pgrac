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
 *	  vtable, Stage 6+ adds RDMA tier vtables; the API surface
 *	  declared here stays unchanged across that evolution.
 *
 *	  Two layers, both exported:
 *
 *	    Low-level byte stream    cluster_ic_send_bytes / recv_bytes
 *	    High-level protocol      cluster_msg_send / cluster_msg_recv
 *	                             cluster_rpc_call (sync request-reply)
 *
 *	  99% of subsystems should use the high-level API.  The byte stream
 *	  is reserved for performance-critical paths that need to bypass
 *	  the protocol layer (e.g. RDMA write zero-copy in Stage 6+).
 *
 *	  Wire format is a 24-byte fixed header (ClusterMsgHeader) followed
 *	  by an opaque payload.  Header layout is anchored by a compile-time
 *	  StaticAssertDecl below; protocol_version=1 is the 0.18 baseline.
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
 * On-the-wire magic.  Little-endian "ICRG" -- Inter-Connect for pgRac
 * Generic.  Chosen for grep-friendly hexdumps (49 52 43 47) and
 * non-collision with PG protocol headers (which use single-byte type
 * codes like 'R' / 'S' / 'E').  Stage 6+ may rev this only via
 * protocol_version, never the magic itself.
 */
#define PGRAC_IC_MAGIC ((uint32)0x47435249)

/*
 * Wire protocol version.  Bumped when ClusterMsgHeader changes shape.
 * Stage 0.18 starts at 1.
 */
#define PGRAC_IC_PROTOCOL_VERSION_V1 ((uint8)1)


/*
 * spec-2.2 §3.9 -- Tier1 scope guard message types.
 *
 * Tier1 transport in spec-2.2 carries ONLY LMON heartbeat traffic.
 * General-purpose backend cluster_msg_send is REJECTED in tier1 mode
 * (caller / msg_type check).  General message routing for cross-node
 * RPC / GES / Cache Fusion / sinval lands later:
 *   spec-2.3 envelope ABI ratify + transport-agnostic API
 *   spec-2.4 framing + epoch enforce
 *   spec-2.X general IC router (TBD)
 *
 * Future spec-2.4+ will add more msg_type values (REQUEST / RESPONSE /
 * INVAL / etc) and lift the scope guard.  Until then, any non-HEARTBEAT
 * msg_type sent through cluster_msg_send in tier1 mode is rejected
 * with ERR_FEATURE_NOT_SUPPORTED.
 */
#define PGRAC_IC_MSG_HEARTBEAT ((uint16)1)

/*
 * Exact size of ClusterMsgHeader, anchored as a constant for unit
 * tests and for cross-checking against StaticAssertDecl in cluster_ic.c.
 */
#define PGRAC_IC_HEADER_BYTES 24


/*
 * ClusterMsgHeader -- fixed wire-format prefix prepended by the
 *	high-level cluster_msg_* API in front of every payload.
 *
 *	Field layout chosen so that the struct is exactly 24 bytes with
 *	natural alignment on every platform pgrac targets.  Reserved
 *	fields are zero-filled by the sender and ignored by the receiver
 *	at protocol_version 1.
 */
typedef struct ClusterMsgHeader {
	uint32 magic;			/* PGRAC_IC_MAGIC */
	uint8 protocol_version; /* PGRAC_IC_PROTOCOL_VERSION_V1 */
	uint8 reserved1;
	uint16 msg_type; /* subsystem-defined, registered in Stage 2+ */
	int16 sender_node_id;
	int16 reserved2;
	uint32 seq_no;		/* per-target monotonic; wraps */
	uint32 payload_len; /* bytes in payload following the header */
	pg_crc32c crc32;	/* CRC32C over (header excl crc) + payload */
} ClusterMsgHeader;


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
 *	Stage 2+ adds ClusterICOps_TCP; Stage 6+ adds Tier 1 / Tier 2 /
 *	Tier 3 RDMA vtables (see interconnect-tier-strategy.md).  The
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
typedef struct ClusterICOps {
	bool (*send_bytes)(int32 target_node_id, const void *buf, size_t len);
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
#define PGRAC_IC_ENVELOPE_VERSION_V1 ((uint16)1) /* spec-2.0 §4 frozen */
#define PGRAC_IC_HELLO_BYTES 64
#define PGRAC_IC_CLUSTER_NAME_MAX 24

typedef struct ClusterICHelloMsg {
	uint32 magic;								  /* PGRAC_IC_HELLO_MAGIC */
	uint16 hello_version;						  /* PGRAC_IC_HELLO_VERSION_V1 */
	uint16 envelope_version;					  /* PGRAC_IC_ENVELOPE_VERSION_V1 */
	int32 source_node_id;						  /* sender's cluster.node_id */
	char cluster_name[PGRAC_IC_CLUSTER_NAME_MAX]; /* NUL-terminated; truncated */
	uint8 _pad[28];								  /* pad to 64B fixed ABI */
} ClusterICHelloMsg;


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
								   const char *cluster_name);
extern bool cluster_ic_parse_hello(const uint8 in_buf[PGRAC_IC_HELLO_BYTES],
								   ClusterICHelloMsg *out_msg);


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
extern bool cluster_ic_send_bytes(int32 target_node_id, const void *buf, size_t len);

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
 * High-level protocol API.
 *
 *	cluster_msg_send -- prepend a 24-byte header (with sender_id,
 *	    seq_no, CRC) and forward to cluster_ic_send_bytes.
 *
 *	cluster_msg_recv -- read next message via cluster_ic_recv_bytes,
 *	    validate magic / protocol_version / CRC, fill *out_hdr.
 *	    Returns false on no message or validation failure (logged
 *	    at WARNING).
 *
 *	cluster_rpc_call -- synchronous request-reply: cluster_msg_send
 *	    followed by cluster_msg_recv loop until matching seq_no or
 *	    timeout_ms expires.  At stage 0.18 stub mode this returns
 *	    false (timeout) for self-targeted RPC, since there is no
 *	    listener producing replies.
 * ----------
 */
extern bool cluster_msg_send(int32 target_node_id, uint16 msg_type, const void *payload,
							 uint32 payload_len);

extern bool cluster_msg_recv(ClusterMsgHeader *out_hdr, void *payload_buf, uint32 payload_buf_size);

extern bool cluster_rpc_call(int32 target_node_id, uint16 msg_type, const void *req, uint32 req_len,
							 void *resp_buf, uint32 resp_buf_size, uint32 *out_resp_len,
							 int timeout_ms);

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
