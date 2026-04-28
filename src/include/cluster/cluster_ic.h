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

#include "port/pg_crc32c.h"


/*
 * ClusterICTier -- the four interconnect tiers defined in
 *	interconnect-tier-strategy.md.  The cluster.interconnect_tier GUC
 *	maps onto this enum; cluster_ic_init picks the corresponding
 *	vtable.  Stage 0.18 only supports CLUSTER_IC_TIER_STUB; tier1
 *	lands in Stage 2, tier2/tier3 in Stage 6+.
 */
typedef enum ClusterICTier {
	CLUSTER_IC_TIER_STUB = 0,
	CLUSTER_IC_TIER_1 = 1,
	CLUSTER_IC_TIER_2 = 2,
	CLUSTER_IC_TIER_3 = 3
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
typedef struct ClusterICOps {
	bool (*send_bytes)(int32 target_node_id, const void *buf, size_t len);
	bool (*recv_bytes)(int32 *out_sender_node_id, void *buf, size_t bufsize,
					   size_t *out_received_len);
	void (*tier_init)(void);
	void (*tier_shutdown)(void);
	const char *tier_name;
} ClusterICOps;

extern const ClusterICOps ClusterICOps_Stub;
extern const ClusterICOps *ClusterICOps_Active;


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

#endif /* CLUSTER_IC_H */
