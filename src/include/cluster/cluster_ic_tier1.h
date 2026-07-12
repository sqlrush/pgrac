/*-------------------------------------------------------------------------
 *
 * cluster_ic_tier1.h
 *	  Internal API of the Tier 1 (TCP) interconnect backend.
 *
 *	  spec-2.2 D4 (NEW; 2026-05-07).  These symbols are intended for
 *	  the LMON aux process ONLY -- per spec-2.2 §3.9 hard scope guard
 *	  the public cluster_ic API (cluster_ic_send_bytes / recv_bytes)
 *	  rejects non-LMON callers when the active tier is tier1.  General-
 *	  purpose backend IC routing lands in spec-2.3 (envelope ABI ratify
 *	  + transport-agnostic API) and spec-2.4 (framing + epoch enforce);
 *	  this header MUST NOT be included from non-LMON sources until then.
 *
 *	  Locking model: tier1's per-peer connection state lives in a single
 *	  shmem region (see ClusterICTier1Shmem in cluster_ic_tier1.c).  All
 *	  state mutation goes through the helpers below and grabs the
 *	  associated LWLock (registered by cluster_ic_tier1_shmem_register).
 *	  Read-only views (pg_cluster_ic_peers SRF in spec-2.2 D9 -- Step 8)
 *	  can take a shared lock to walk the array.
 *
 *	  Per-peer file descriptors are NOT in shmem (kernel resources are
 *	  per-process); they live in a static array within the LMON process
 *	  and are managed exclusively by LMON.  Vtable functions in tier1.c
 *	  (called from cluster_ic_send_bytes etc) read this static array.
 *
 *	  Spec authority: pgrac/specs/spec-2.2-interconnect-tcp-listener-
 *	  lmon-phase1.md v0.1 frozen (commit 3819f1ac36 in pgrac).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ic_tier1.h
 *
 * NOTES
 *	  pgrac-original file.  Built only in --enable-cluster mode; not
 *	  referenced by pg_proc.dat (LMON-internal only).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_IC_TIER1_H
#define CLUSTER_IC_TIER1_H

#include "cluster/cluster_ic.h"


#ifdef USE_PGRAC_CLUSTER

/*
 * Per-peer state in shmem.  Per spec-2.2 §2.6 frozen layout
 * (256 bytes per peer, cache-line aligned to keep cross-CPU
 * peer-state writes from sharing cache lines).
 */
#include "datatype/timestamp.h" /* TimestampTz */
#include "port/atomics.h"		/* pg_atomic_uint64 */

#define CLUSTER_IC_TIER1_LAST_ERROR_LEN 128
#define CLUSTER_IC_TIER1_ADDR_LEN 64

typedef struct ClusterICPeerStateShmem {
	int32 node_id; /* peer's node_id */
	int32 state;   /* ClusterICPeerState */
	char interconnect_addr[CLUSTER_IC_TIER1_ADDR_LEN];
	TimestampTz last_connect_at;
	TimestampTz last_send_at;
	TimestampTz last_recv_at;
	TimestampTz last_heartbeat_sent_at;
	TimestampTz last_heartbeat_recv_at;
	pg_atomic_uint64 heartbeat_send_count;
	pg_atomic_uint64 heartbeat_recv_count;
	pg_atomic_uint64 msg_send_count;
	pg_atomic_uint64 msg_recv_count;
	pg_atomic_uint64 bytes_send;
	pg_atomic_uint64 bytes_recv;
	uint32 reconnect_count;
	uint32 connect_error_count;
	int32 last_errno;
	char last_error_code[8]; /* SQLSTATE + NUL */
	char last_error[CLUSTER_IC_TIER1_LAST_ERROR_LEN];
	/* spec-2.4 D10: 4 NEW per-peer atomic counters */
	pg_atomic_uint64 stale_epoch_drop_count;		 /* envelope verify step 7 reject (env < my) */
	pg_atomic_uint64 lamport_observe_advance_count;	 /* envelope_observe_scn advance */
	pg_atomic_uint64 chunk_reassembly_timeout_count; /* spec-2.4 D6 timeout cleanup */
	pg_atomic_uint32 chunk_reassembly_active;		 /* in-flight reassembly seq_next */
	/* spec-2.29 D20 hostile-spoof defense: env_epoch - my_epoch > MAX_JUMP. */
	pg_atomic_uint64 unreasonable_epoch_jump_count; /* envelope verify step 7 reject (> MAX_JUMP) */
	/* spec-2.29 D18b: my_epoch advanced via envelope-piggyback observe. */
	pg_atomic_uint64 epoch_observe_advance_count; /* env_epoch > my_epoch, advanced */
	/*
	 * spec-2.4 hardening v1.0.1 F3 (L74 cross-aux-process-close-must-be-LMON-mediated):
	 * close_requested = 1 when non-LMON aux context wants to close the peer.
	 * LMON main tick reads, performs actual close + lmon_peer_track sync,
	 * then clears.  This makes LMON the sole owner of fd/state machine
	 * lifecycle even when CSSD / chunk-timeout / future GES wants to
	 * close.
	 */
	pg_atomic_uint32 close_requested; /* 0 = idle, 1 = pending close */

	/*
	 * PGRAC: spec-7.2 D5 (INV-7.2-CONN-EPOCH) — the cluster epoch this
	 * connection was established under (recorded at the CONNECTED
	 * transition;  DATA plane only, CONTROL leaves it 0/unenforced).
	 * The DATA physical-send gate refuses to put bytes on a connection
	 * whose conn_epoch != current epoch — an epoch bump structurally
	 * invalidates the old stream and forces close + re-HELLO, so a new
	 * epoch's frames can never interleave into an old epoch's byte
	 * stream.  Written and read by the owning plane's process only.
	 */
	uint64 conn_epoch;
} ClusterICPeerStateShmem;

/*
 * Shmem region registration -- called by cluster_shmem_register_region
 * during cluster_init_shmem_module (per spec-1.3 framework).  Real
 * shmem allocation happens in tier1_shmem_init (cluster_ic_tier1.c).
 */
extern void cluster_ic_tier1_shmem_register(void);


/*
 * Postmaster / LMON aux process API.  All functions return false on
 * failure with errno preserved + last_error / last_errno written to
 * the relevant peer slot (caller can read via cluster_ic_tier1_peer_get).
 *
 * Per spec-2.2 §3.10:
 *   - cluster_ic_tier1_listener_bind() FATAL on failure (only failure
 *     point that escalates to FATAL; called once at LMON main entry)
 *   - All other helpers handle failure via peer state + last_error;
 *     never FATAL the postmaster
 */

/*
 * Resolve self interconnect_addr from pgrac.conf, open a nonblocking
 * TCP listener, set SO_REUSEADDR, bind, listen.  Returns the listener
 * fd (>= 0) on success.  FATAL on failure -- this is the only path
 * in spec-2.2 where transport setup may FATAL the postmaster (per
 * §3.10 invariant; LMON cannot do its job without a listener).
 *
 * Stores the fd in shmem (visible to vtable functions) AND returns it
 * to caller for direct LMON WaitEventSet registration.
 */
extern int cluster_ic_tier1_listener_bind(void);

/*
 * PGRAC: spec-7.2 D2 — re-aim this process's tier1 working state at a
 * plane ([0] = CONTROL, [1] = DATA inside the single tier1 region).
 * Call once at aux-process entry BEFORE any tier1 use (LmsMain → DATA);
 * processes that never call it stay on CONTROL (LMON, backends, SQL
 * observers — the historic instance and contract).
 */
extern void cluster_ic_tier1_set_my_plane(ClusterICPlane plane);
extern ClusterICPlane cluster_ic_tier1_my_plane(void);

/*
 * PGRAC: spec-7.3 D3 — the DATA plane is parallelised across per-worker
 * channels.  CLUSTER_IC_TIER1_DATA_CHANNELS is the compile-time cap on DATA
 * worker channels the transport carves shmem for (one ClusterICTier1Shmem
 * instance each, so every worker has private peers[] / listener metadata and
 * the send-gate is not clobbered across worker processes).  It must be >=
 * the LMS worker cap CLUSTER_LMS_MAX_WORKERS (asserted in cluster_lms.c).
 *
 *	Channel 0 is worker 0 (the LmsProcess DATA plane) and keeps the spec-7.2
 *	shmem offset, so cluster.lms_workers = 1 is byte-identical to spec-7.2.
 *
 * set_my_data_channel(channel, n_workers) implies the DATA plane and re-aims
 * this process's tier1 working state at channel's private instance;  the
 * listener/dial port become (declared data port + channel), and the HELLO
 * carries (channel as worker_id, n_workers) for the D3 negotiation.  A DATA
 * process that only calls set_my_plane(DATA) stays on channel 0 (worker 0).
 */
#define CLUSTER_IC_TIER1_DATA_CHANNELS 8
extern void cluster_ic_tier1_set_my_data_channel(int channel, int n_workers);
extern int cluster_ic_tier1_my_data_channel(void);
extern int cluster_ic_tier1_my_n_workers(void);

/* PGRAC: spec-7.2 D3 — dispatch plane-gate drop counter (per plane). */
extern void cluster_ic_tier1_bump_plane_misroute_reject(void);
extern uint64 cluster_ic_tier1_get_plane_misroute_reject(ClusterICPlane plane);

/*
 * Accept exactly one pending connection on the listener fd.  Returns
 * true if accepted (sets *out_peer_fd and *out_peer_id from HELLO once
 * verified), false if no connection pending (EAGAIN) or HELLO failed.
 *
 * Caller responsible for adding *out_peer_fd to its WaitEventSet on
 * success.  On HELLO failure (per §3.10) the connection is closed
 * INTERNALLY and peer state for the rejecting peer (if known) is
 * updated with last_error_code; this function returns false.
 */
extern bool cluster_ic_tier1_accept_one(int *out_peer_fd, int32 *out_peer_id);

/*
 * Issue a nonblocking connect(2) to peer_id's interconnect_addr.
 * Mesh role check (§3.5) -- caller is expected to call this only for
 * peers where cluster_ic_mesh_role_for_pair returns ACTIVE.  Returns
 * true with *out_peer_fd set if connect succeeded immediately or is
 * in progress (EINPROGRESS); the LMON wait loop then watches the fd
 * for writeability + calls cluster_ic_tier1_finish_connect.
 *
 * On hard error (e.g. bad address), updates peer state to DOWN with
 * last_errno + last_error and returns false.
 */
extern bool cluster_ic_tier1_connect_one(int32 peer_id, int *out_peer_fd);

/*
 * Called when the in-progress connect socket becomes writeable.  Checks
 * SO_ERROR; on success sends HELLO + advances peer state to CONNECTING.
 * On socket error or HELLO send failure, closes the fd + state DOWN.
 */
extern bool cluster_ic_tier1_finish_connect(int32 peer_id, int peer_fd);

/*
 * Called when a CONNECTING peer fd becomes readable -- attempt to
 * recv + verify HELLO from peer.  On success advances peer state to
 * CONNECTED and updates last_connect_at.  On HELLO mismatch (wrong
 * cluster_name / source_node_id / version), updates state to REJECTED
 * with last_error_code = '08P01' and closes the fd (per §3.10).
 */
extern bool cluster_ic_tier1_recv_and_verify_hello(int32 peer_id, int peer_fd);

/*
 * spec-2.2 additive amendment (spec-5.22e D5 prereq): register the
 * PEER_CAPS_REPLY msg_type + dialer-side handler (LMON-only producer;
 * p2p, never broadcast).  Called from LMON's phase-1 registration block.
 */
extern void cluster_ic_tier1_register_caps_reply_msg_type(void);

/*
 * Send a HEARTBEAT message to a CONNECTED peer (no-op for non-CONNECTED).
 * Updates heartbeat_send_count + last_heartbeat_sent_at on DONE.
 *
 * Per spec-2.2 §3.6 boundary invariant, heartbeats only signal IC
 * transport liveness -- DO NOT trigger fence / membership change /
 * quorum decision from heartbeat results.
 *
 * spec-2.3 hardening v1.0.1 F1 (L68): three-state return.  Caller
 * (LMON main loop) MUST switch on result.  WOULD_BLOCK means the
 * outbound buffer holds the tail; LMON should register
 * WL_SOCKET_WRITEABLE for the fd, NOT close peer.
 */
extern ClusterICSendResult cluster_ic_tier1_send_heartbeat(int32 peer_id);

/*
 * spec-2.3 hardening v1.0.1 F1: pending-outbound accessor for LMON.
 * Returns true iff this peer has bytes queued in tier1_outbound_buf
 * waiting for WL_SOCKET_WRITEABLE.  LMON uses this to decide whether
 * to register WRITEABLE interest in its WaitEventSet.
 */
extern bool cluster_ic_tier1_pending_outbound(int32 peer_id);

/*
 * PGRAC: GCS-race round-4c tier1-partial-IO F2 — standalone drain entry for
 * a WL_SOCKET_WRITEABLE wakeup on a CONNECTED peer with pending outbound
 * bytes.  Pushes ONLY the backpressured tail (never injects a new frame —
 * unlike the CONTROL plane's idempotent-heartbeat re-entry).  DONE = tail
 * fully drained (drop WRITEABLE interest);  WOULD_BLOCK = still
 * backpressured (keep it);  HARD_ERROR = dead socket / corrupt drain state
 * (caller must close the peer).  get_writable_drain aggregates the drain
 * wakeup counter per plane (DATA sums its worker channels).
 */
extern ClusterICSendResult cluster_ic_tier1_drain_outbound(int32 peer_id);
extern uint64 cluster_ic_tier1_get_writable_drain(ClusterICPlane plane);

/*
 * spec-2.4 D10 per-peer counter bumpers.  Used by envelope_verify
 * (stale_epoch_drop), envelope_observe_scn (lamport_advance), and
 * cluster_ic_chunk reassembly state machine (timeout / active).
 *
 * Range-checked internally;peer_id out of [0, CLUSTER_MAX_NODES) is
 * a no-op (defensive when called pre-handshake or in unit-test).
 */
extern void cluster_ic_tier1_bump_stale_epoch_drop(int32 peer_id);
extern void cluster_ic_tier1_bump_lamport_advance(int32 peer_id);
extern void cluster_ic_tier1_bump_chunk_reassembly_timeout(int32 peer_id);
/* spec-2.29 D20: hostile-spoof defense + epoch piggyback observe counters. */
extern void cluster_ic_tier1_bump_unreasonable_epoch_jump(int32 peer_id);
extern void cluster_ic_tier1_bump_epoch_observe_advance(int32 peer_id);
extern void cluster_ic_tier1_set_chunk_reassembly_active(int32 peer_id, uint32 active);

/*
 * spec-2.4 hardening v1.0.1 F3 (L74): non-LMON callers (chunk timeout,
 * future CSSD / GES) that want to close a peer go through this API.
 * Sets close_requested in shmem;LMON main tick observes, performs the
 * actual close + lmon_peer_track sync, then clears.  reason is logged
 * immediately by the requester (not stored in shmem).
 *
 * Idempotent;repeated requests collapse into one close.
 */
extern void cluster_ic_tier1_request_close_peer(int32 peer_id, const char *reason);

/*
 * spec-2.4 hardening v1.0.1 F3: LMON main tick scans peers for
 * close_requested = 1, performs actual close + clears flag.  Returns
 * true if any close was performed (LMON should set wes_dirty).
 */
extern bool cluster_ic_tier1_lmon_drain_close_requests(void);

/*
 * Hardening v1.0.1 F1: continue an in-progress HELLO send (active side).
 * Called by LMON from finish_connect AND on each WL_SOCKET_WRITEABLE
 * wakeup until HELLO is fully sent.  Returns true on success / partial;
 * false on hard error.  When the full 64 bytes have been sent, the
 * peer's shmem state flips to CONNECTED.
 *
 * Use cluster_ic_tier1_hello_send_remaining() to learn whether more
 * sends are needed (returns 0 = complete, > 0 = remaining bytes).
 */
extern bool cluster_ic_tier1_continue_hello_send(int32 peer_id, int peer_fd);
extern int cluster_ic_tier1_hello_send_remaining(int32 peer_id);

/*
 * Hardening v1.0.1 F1: passive-side HELLO recv with partial-recv
 * accumulation.  Called by LMON on WL_SOCKET_READABLE for
 * accept-side anonymous fds before peer_id is known.
 *
 *   anon_slot         : 0..CLUSTER_MAX_NODES-1, identifies the
 *                       per-call accumulation buffer (LMON owns
 *                       the slot until HELLO complete or fd closed).
 *   peer_fd           : the fd to recv from
 *   out_learned_peer_id : on success of full HELLO, set to verified
 *                       source_node_id (caller binds peer fd).  If
 *                       still accumulating, set to -1.
 *
 * Returns true while recv is making progress (partial or complete);
 * false on hard error / EOF / verification failure -- caller closes fd.
 *
 * Caller must reset accumulation state when fd is bound or closed
 * via cluster_ic_tier1_anon_hello_reset(anon_slot).
 */
extern bool cluster_ic_tier1_continue_hello_recv(int anon_slot, int peer_fd,
												 int32 *out_learned_peer_id);
extern void cluster_ic_tier1_anon_hello_reset(int anon_slot);

/*
 * Drain pending heartbeat frames from peer_fd.  Reads until EAGAIN,
 * accumulating bytes into a per-peer recv buffer; for each fully
 * received 24-byte ClusterMsgHeader (msg_type=HEARTBEAT, payload_len=0,
 * CRC OK) bumps heartbeat_recv_count + last_heartbeat_recv_at.
 * Returns false on hard recv error (caller should close_peer).
 */
extern bool cluster_ic_tier1_recv_heartbeat_drain(int32 peer_id, int peer_fd);

/*
 * Mark a peer's connection as lost (state DOWN, close fd, schedule
 * reconnect via exponential backoff handled by LMON main loop).
 * Idempotent; safe to call repeatedly during shutdown.
 */
extern void cluster_ic_tier1_close_peer(int32 peer_id, const char *reason);

/*
 * Read-only access to peer state shmem entry (for view + LMON reads).
 * Returns NULL if peer_id out of range or pgrac.conf doesn't declare
 * this peer.  Caller may take cluster_ic_tier1_lwlock_shared if it
 * needs a consistent snapshot across multiple peers.
 */
extern const ClusterICPeerStateShmem *cluster_ic_tier1_peer_get(int32 peer_id);

/*
 * Get the listener fd (for LMON WaitEventSet registration).  Returns
 * -1 if listener not yet bound.
 */
extern int cluster_ic_tier1_get_listener_fd(void);

/*
 * Per-peer fd accessor.  Returns -1 if peer not currently connected.
 * Process-local (only valid in the LMON aux process where listener
 * was bound).
 */
extern int cluster_ic_tier1_get_peer_fd(int32 peer_id);

/*
 * Hardening v1.0.1 F3: listener metadata accessors -- read from shmem
 * (visible to any backend; not the fd itself which is process-local).
 * pid = LMON pid that opened the current listener fd; incarnation
 * monotonically increments on every successful listener_bind, so
 * observers can detect "this LMON has respawned and rebound".
 * Returns 0/0 if listener has never been bound.
 */
extern pid_t cluster_ic_tier1_get_listener_pid(void);
extern uint64 cluster_ic_tier1_get_listener_incarnation(void);
extern int cluster_ic_tier1_get_listener_port(void);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_IC_TIER1_H */
