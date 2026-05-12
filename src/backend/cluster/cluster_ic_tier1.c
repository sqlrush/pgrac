/*-------------------------------------------------------------------------
 *
 * cluster_ic_tier1.c
 *	  Tier 1 (TCP) interconnect backend.
 *
 *	  spec-2.2 D3 (NEW; 2026-05-07).  Replaces the transitional
 *	  ClusterICOps_Tier1 stub previously living at the bottom of
 *	  cluster_ic.c (must be removed in the same commit; otherwise
 *	  the linker reports a duplicate-symbol error on
 *	  ClusterICOps_Tier1).
 *
 *	  Spec authority: pgrac/specs/spec-2.2-interconnect-tcp-listener-
 *	  lmon-phase1.md v0.1 frozen (commit 3819f1ac36 in pgrac).
 *
 *	  HARD INVARIANT scope guard (§3.9): this file is wired into
 *	  ClusterICOps_Active when cluster.interconnect_tier = tier1 AND
 *	  cluster_enabled = true.  cluster_ic_send_bytes (cluster_ic.c)
 *	  enforces caller / msg_type restrictions (LMON only;
 *	  HEARTBEAT msg_type only); other backends in tier1 mode receive
 *	  ERR_FEATURE_NOT_SUPPORTED.  Step 9 (D2 §3.9) lands the runtime
 *	  enforcement; this file does not need to re-check.
 *
 *	  SCOPE: Step 6 (this commit) ships the file plus its public
 *	  surface (vtable + LMON-internal helpers) and the shmem region.
 *	  Step 7 (D5+D6) wires LMON's main loop to call the helpers via
 *	  WaitEventSet.  Step 6 standalone leaves listener_fd unbound
 *	  and per-peer fds at -1 (no traffic until Step 7) -- but the
 *	  vtable + helper symbols are link-time complete so cluster_unit
 *	  test_tier1_vtable_extern_linkable passes and existing 014/072
 *	  TAP regression doesn't break.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ic_tier1.c
 *
 * NOTES
 *	  pgrac-original.  Compiled only in --enable-cluster builds
 *	  (cluster/Makefile OBJS).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <netinet/tcp.h> /* TCP_KEEPIDLE / TCP_KEEPALIVE / KEEPINTVL / KEEPCNT */
#include <unistd.h>
#include <errno.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h" /* WAIT_EVENT_CLUSTER_IC_* (Hardening v1.0.1 F4) */
#include "pgstat.h"			  /* pgstat_report_wait_start/end */

#include "cluster/cluster_conf.h"
#include "cluster/cluster_elog.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_chunk.h" /* cluster_ic_chunk_reset_peer (spec-2.4) */
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_shmem.h"

/*
 * PG_FUNCTION_INFO_V1(cluster_get_ic_peers) lives in cluster_ic.c
 * (always-linked file); cluster_ic_tier1.c provides the
 * USE_PGRAC_CLUSTER body, cluster_ic.c provides the disable-cluster
 * stub.
 */


#ifdef USE_PGRAC_CLUSTER

/* ============================================================
 * Shmem layout.
 * ============================================================ */

typedef struct ClusterICTier1Shmem {
	/*
	 * Listener metadata only -- the actual listener fd is process-local
	 * in LMON (see tier1_listener_fd below).  Hardening v1.0.1 F3:
	 * fd is a process-local kernel resource; the integer that names it
	 * is meaningful only in the process that opened it.  After LMON
	 * respawn the old fd value is closed (or, worse, reassigned to an
	 * unrelated fd in the new process).  Storing fd in shmem caused
	 * silent listener failure on respawn.  Now shmem holds only
	 * (port, owner pid, incarnation counter); the fd lives in
	 * tier1_listener_fd in the running LMON process.
	 */
	int listener_port;			 /* cached self port */
	pid_t listener_pid;			 /* current LMON pid */
	uint64 listener_incarnation; /* ++ on each LMON respawn */
	uint32 magic;				 /* sanity check */
	ClusterICPeerStateShmem peers[CLUSTER_MAX_NODES];
} ClusterICTier1Shmem;

#define PGRAC_IC_TIER1_SHMEM_MAGIC ((uint32)0x54494331U) /* "TIC1" */

static ClusterICTier1Shmem *Tier1Shmem = NULL;

/*
 * Listener fd -- process-local; valid only in the LMON aux process that
 * called listener_bind().  Shmem stores listener_pid + incarnation so
 * other backends can observe "which LMON owns this listener" for
 * diagnostic views, but the fd itself is never crossed between
 * processes (Hardening v1.0.1 F3).
 */
static int tier1_listener_fd = -1;

/*
 * Per-peer fds (process-local, valid only in LMON aux process where
 * the listener was bound).  Vtable send/recv functions read this
 * array; only LMON ever mutates it.
 */
static int tier1_peer_fds[CLUSTER_MAX_NODES];
static bool tier1_peer_fds_initialised = false;

/*
 * Per-peer recv buffer for accumulating partial ClusterMsgHeader frames
 * across multiple WL_SOCKET_READABLE wakeups.  TCP can deliver bytes
 * in any chunk size; the LMON loop reads what's available and parses
 * complete frames as they assemble.  Process-local (LMON only).
 */
/*
 * spec-2.3 D6: per-peer recv buffer for accumulating partial 36-byte
 * envelopes across multiple WL_SOCKET_READABLE wakeups.  Replaces
 * spec-2.2's 24-byte ClusterMsgHeader buffer.  TCP can deliver bytes
 * in any chunk size; LMON loop reads what's available and parses
 * complete envelopes as they assemble.  v1.0.1 F1 partial-IO state
 * machine pattern preserved (per-peer accumulator + len counter).
 */
static uint8 tier1_recv_buf[CLUSTER_MAX_NODES][PGRAC_IC_ENVELOPE_BYTES];
static int tier1_recv_buf_len[CLUSTER_MAX_NODES];

/*
 * Hardening v1.0.1 F1: per-peer HELLO send + recv buffers + state for
 * partial-IO across WL_SOCKET_WRITEABLE / READABLE wakeups.  64-byte
 * HELLO can fragment on real LANs (and almost-always-doesn't on
 * loopback, hiding the bug).  state machine:
 *   active : finish_connect() seeds tier1_hello_send_buf[],
 *            tier1_hello_send_remaining = 64; LMON re-enters
 *            cluster_ic_tier1_continue_hello_send() on WRITEABLE
 *            until remaining == 0; then peer state -> CONNECTED.
 *   passive: cluster_ic_tier1_recv_and_verify_hello() accumulates
 *            into tier1_hello_recv_buf[] until len == 64; then
 *            parses + verifies + state -> CONNECTED.
 *
 * Heartbeat send partial-write: tier1_outbound_remaining[] stores
 * leftover bytes when send() returns short; LMON drains on WRITEABLE
 * before scheduling next heartbeat.  For HEARTBEAT (24B header,
 * 0B payload) the buffer reuses tier1_outbound_buf[] (24 bytes per
 * peer); spec-2.4 framing will generalize for larger payloads.
 */
static uint8 tier1_hello_send_buf[CLUSTER_MAX_NODES][PGRAC_IC_HELLO_BYTES];
static int tier1_hello_send_remaining[CLUSTER_MAX_NODES];

/*
 * Anon-slot keyed buffer (passive-side HELLO recv before peer_id known).
 * LMON owns the slot 0..N-1 mapping to lmon_pending_fds[].  After HELLO
 * verifies and fd is bound to peer, LMON calls anon_hello_reset to free
 * the slot for next accept.
 */
static uint8 tier1_anon_hello_buf[CLUSTER_MAX_NODES][PGRAC_IC_HELLO_BYTES];
static int tier1_anon_hello_len[CLUSTER_MAX_NODES];

/*
 * spec-2.4 hardening v1.0.1 F2: outbound tail buffer is now dynamic
 * per-peer.  Lazy-palloc on first partial-write up to PGRAC_IC_PAYLOAD_MAX
 * (16 MB).  pfree on peer close (close_peer F5 fix).  Replaces spec-2.2
 * v1.0.1 static 36-byte buffer that capped chunk frames at HARD_ERROR.
 */
static uint8 *tier1_outbound_buf_dyn[CLUSTER_MAX_NODES];
static int tier1_outbound_buf_dyn_size[CLUSTER_MAX_NODES];
static int tier1_outbound_remaining[CLUSTER_MAX_NODES];

/*
 * spec-2.4 hardening v1.0.1 F1: variable-length payload recv state.
 *   tier1_recv_phase[peer]: 0 = filling tier1_recv_buf[peer] (36 B envelope)
 *                           1 = envelope assembled, filling tier1_recv_payload_buf_dyn
 *   tier1_recv_payload_buf_dyn[peer]: lazy palloc'd up to PGRAC_IC_PAYLOAD_MAX
 *   tier1_recv_payload_total[peer]:    expected payload byte count from envelope
 *   tier1_recv_payload_filled[peer]:   bytes read into payload buf so far
 *
 * After dispatch (or peer close) all four reset.
 */
static int tier1_recv_phase[CLUSTER_MAX_NODES];
static uint8 *tier1_recv_payload_buf_dyn[CLUSTER_MAX_NODES];
/* spec-2.5 hardening v1.0.1 F3 (L80 dynamic-buffer-must-track-capacity):
 * track per-peer allocated capacity so a frame whose payload exceeds the
 * existing buffer triggers grow/realloc rather than overrun.  Without
 * this the lazy-palloc-on-NULL path locks the buffer to the FIRST frame's
 * payload size;subsequent larger frames overwrite past the allocation. */
static int tier1_recv_payload_buf_dyn_capacity[CLUSTER_MAX_NODES];
static int tier1_recv_payload_total[CLUSTER_MAX_NODES];
static int tier1_recv_payload_filled[CLUSTER_MAX_NODES];


/* ============================================================
 * Static helpers.
 * ============================================================ */

static inline void
peer_fds_lazy_init(void)
{
	if (!tier1_peer_fds_initialised) {
		int i;

		for (i = 0; i < CLUSTER_MAX_NODES; i++)
			tier1_peer_fds[i] = -1;
		tier1_peer_fds_initialised = true;
	}
}

static int
set_socket_nonblocking(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);

	if (flags == -1)
		return -1;
	return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/*
 * spec-2.4 D8 -- apply TCP KeepAlive setsockopt 4 options on a fresh
 * peer fd.  Best-effort:warn on failure but DO NOT close the
 * connection (KeepAlive is a kernel-level fallback to spec-2.2 v1.0.1
 * F2 application-level 3x heartbeat liveness scan;app dead detection
 * is the primary defense).
 *
 * Linux: TCP_KEEPIDLE / macOS: TCP_KEEPALIVE alias (same semantics).
 */
static void
apply_tcp_keepalive(int fd, const char *peer_label)
{
	int yes = 1;
	int idle = cluster_interconnect_tcp_keepidle_sec;
	int intvl = cluster_interconnect_tcp_keepintvl_sec;
	int cnt = cluster_interconnect_tcp_keepcnt;

	if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) < 0)
		ereport(WARNING,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1 SO_KEEPALIVE setsockopt failed for %s: %m", peer_label)));

#if defined(TCP_KEEPIDLE)
	if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle)) < 0)
		ereport(WARNING,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1 TCP_KEEPIDLE setsockopt failed for %s: %m", peer_label)));
#elif defined(TCP_KEEPALIVE)
	/* macOS alias for TCP_KEEPIDLE. */
	if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &idle, sizeof(idle)) < 0)
		ereport(WARNING, (errcode_for_socket_access(),
						  errmsg("cluster_ic tier1 TCP_KEEPALIVE setsockopt failed for %s: %m",
								 peer_label)));
#endif

#ifdef TCP_KEEPINTVL
	if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl)) < 0)
		ereport(WARNING, (errcode_for_socket_access(),
						  errmsg("cluster_ic tier1 TCP_KEEPINTVL setsockopt failed for %s: %m",
								 peer_label)));
#endif

#ifdef TCP_KEEPCNT
	if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt)) < 0)
		ereport(WARNING,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1 TCP_KEEPCNT setsockopt failed for %s: %m", peer_label)));
#endif
}

/*
 * Parse "host:port" form into separate fields.  Returns true on
 * success.  The cluster_conf parser (spec-0.19) already validates
 * the format at config-load time, but we re-split here for use with
 * getaddrinfo / sockaddr.
 */
static bool
parse_host_port(const char *addr, char *out_host, size_t host_size, int *out_port)
{
	const char *colon;
	size_t host_len;
	long port;
	char *endp;

	if (addr == NULL || addr[0] == '\0')
		return false;

	colon = strrchr(addr, ':'); /* rightmost ':' to support IPv6 [..] later */
	if (colon == NULL || colon == addr)
		return false;

	host_len = colon - addr;
	if (host_len + 1 > host_size)
		return false;

	memcpy(out_host, addr, host_len);
	out_host[host_len] = '\0';

	port = strtol(colon + 1, &endp, 10);
	if (*endp != '\0' || port < 1 || port > 65535)
		return false;

	*out_port = (int)port;
	return true;
}

/*
 * Update last_error fields on a peer slot.  Caller holds whatever
 * lock its convention requires; this is just the field-write helper.
 */
static void peer_record_error(int32 peer_id, int saved_errno, const char *errcode_str,
							  const char *fmt, ...) pg_attribute_printf(4, 5);

static void
peer_record_error(int32 peer_id, int saved_errno, const char *errcode_str, const char *fmt, ...)
{
	ClusterICPeerStateShmem *p;
	va_list ap;

	if (Tier1Shmem == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;

	p = &Tier1Shmem->peers[peer_id];
	p->last_errno = saved_errno;
	if (errcode_str != NULL) {
		strlcpy(p->last_error_code, errcode_str, sizeof(p->last_error_code));
	}

	va_start(ap, fmt);
	vsnprintf(p->last_error, sizeof(p->last_error), fmt, ap);
	va_end(ap);
}

/*
 * Find a peer's interconnect_addr from pgrac.conf shmem.  Caches
 * into the peer state slot the first time we see it (so the view
 * always has a populated interconnect_addr even before connect).
 */
static const char *
peer_addr(int32 peer_id)
{
	const ClusterNodeInfo *n;

	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return NULL;

	n = cluster_conf_lookup_node(peer_id);
	if (n == NULL)
		return NULL;

	if (Tier1Shmem != NULL && Tier1Shmem->peers[peer_id].interconnect_addr[0] == '\0') {
		strlcpy(Tier1Shmem->peers[peer_id].interconnect_addr, n->interconnect_addr,
				sizeof(Tier1Shmem->peers[peer_id].interconnect_addr));
		Tier1Shmem->peers[peer_id].node_id = peer_id;
	}
	return n->interconnect_addr;
}


/* ============================================================
 * Shmem region (per spec-1.3 cluster_shmem framework).
 * ============================================================ */

static Size
tier1_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterICTier1Shmem));
}

static void
tier1_shmem_init(void)
{
	bool found;

	Tier1Shmem = (ClusterICTier1Shmem *)ShmemInitStruct("pgrac cluster_ic_tier1",
														tier1_shmem_size(), &found);

	if (!found) {
		int i;

		memset(Tier1Shmem, 0, tier1_shmem_size());
		Tier1Shmem->magic = PGRAC_IC_TIER1_SHMEM_MAGIC;
		Tier1Shmem->listener_port = -1;
		Tier1Shmem->listener_pid = 0;
		Tier1Shmem->listener_incarnation = 0;

		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			Tier1Shmem->peers[i].node_id = -1;
			Tier1Shmem->peers[i].state = (int32)CLUSTER_IC_PEER_DOWN;
			Tier1Shmem->peers[i].last_errno = 0;
			Tier1Shmem->peers[i].last_connect_at = 0;
			pg_atomic_init_u64(&Tier1Shmem->peers[i].heartbeat_send_count, 0);
			pg_atomic_init_u64(&Tier1Shmem->peers[i].heartbeat_recv_count, 0);
			pg_atomic_init_u64(&Tier1Shmem->peers[i].msg_send_count, 0);
			pg_atomic_init_u64(&Tier1Shmem->peers[i].msg_recv_count, 0);
			pg_atomic_init_u64(&Tier1Shmem->peers[i].bytes_send, 0);
			pg_atomic_init_u64(&Tier1Shmem->peers[i].bytes_recv, 0);
			/* spec-2.4 D10 NEW counters */
			pg_atomic_init_u64(&Tier1Shmem->peers[i].stale_epoch_drop_count, 0);
			pg_atomic_init_u64(&Tier1Shmem->peers[i].lamport_observe_advance_count, 0);
			pg_atomic_init_u64(&Tier1Shmem->peers[i].chunk_reassembly_timeout_count, 0);
			pg_atomic_init_u32(&Tier1Shmem->peers[i].chunk_reassembly_active, 0);
			/* spec-2.29 D20 + D18b: hostile-spoof + epoch piggyback counters. */
			pg_atomic_init_u64(&Tier1Shmem->peers[i].unreasonable_epoch_jump_count, 0);
			pg_atomic_init_u64(&Tier1Shmem->peers[i].epoch_observe_advance_count, 0);
			/* spec-2.4 v1.0.1 F3: LMON-mediated close request flag. */
			pg_atomic_init_u32(&Tier1Shmem->peers[i].close_requested, 0);
		}
	}

	Assert(Tier1Shmem->magic == PGRAC_IC_TIER1_SHMEM_MAGIC);
}

static const ClusterShmemRegion cluster_ic_tier1_region = {
	.name = "pgrac cluster_ic_tier1",
	.size_fn = tier1_shmem_size,
	.init_fn = tier1_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_ic_tier1",
	.reserved_flags = 0,
};

void
cluster_ic_tier1_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ic_tier1_region);
}


/* ============================================================
 * Vtable hooks (called from cluster_ic.c through ClusterICOps_Active
 * when cluster.interconnect_tier = tier1).
 * ============================================================ */

static ClusterICSendResult
tier1_send_bytes(int32 target_node_id, const void *buf, size_t len)
{
	int fd;
	ssize_t sent;

	peer_fds_lazy_init();

	if (target_node_id < 0 || target_node_id >= CLUSTER_MAX_NODES) {
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_ic tier1 send: target_node_id %d out of range", target_node_id)));
		return CLUSTER_IC_SEND_HARD_ERROR;
	}

	fd = tier1_peer_fds[target_node_id];
	if (fd < 0)
		return CLUSTER_IC_SEND_HARD_ERROR; /* not connected */

	/*
	 * spec-2.13 Hardening v1.0.2 root-cause fix (L66-family真根因):
	 * peer must be in CLUSTER_IC_PEER_CONNECTED state (HELLO handshake
	 * completed both directions) before any envelope-class send.
	 *
	 *	Without this guard:  tier1_peer_fds[peer] becomes valid (>=0)
	 *	the moment cluster_ic_tier1_connect_one() creates the socket,
	 *	BEFORE finish_connect() / continue_hello_send() puts HELLO 64
	 *	bytes on the wire.  Concurrent callers (CSSD outbound drain at
	 *	cluster_lmon.c:984 — only guard is fd >= 0;  spec-2.5 v1.0.1 F2
	 *	combined-send path) can race in this window and write an envelope
	 *	frame (msg_type=11 CSSD_HEARTBEAT etc) to the socket BEFORE the
	 *	HELLO bytes — node1's passive HELLO recv reads the envelope as
	 *	the would-be HELLO,  parse_hello sees magic 0x4943 (envelope)
	 *	instead of 0x4F4C4C48 (HELLO) → bad magic → close peer.  Loops
	 *	every CSSD heartbeat tick (1Hz) until eventually a tick happens
	 *	to fall after HELLO completes naturally on slow CI runners.
	 *
	 *	Observed wire hex (nightly run 25735044793, Ubuntu CI):
	 *	  43 49 01 0b 00 00 00 00 01 00 00 00 ...
	 *	  = envelope magic 0x4943 + version 1 + msg_type 11 (CSSD_HB)
	 *	  + source_node_id=0 + dest_node_id=1 + epoch=0...
	 *
	 *	tier1_send_heartbeat (this file, line 1357-1362) already guards
	 *	on state CONNECTED before calling cluster_ic_send_envelope.
	 *	CSSD's outbound drain in cluster_lmon.c calls cluster_ic_send_bytes
	 *	directly (combined env+payload single-buffer per L79), bypassing
	 *	that guard.  Moving the guard inside tier1_send_bytes is the
	 *	single defense for all caller paths (CSSD now + future GES /
	 *	SCN_BROADCAST / SI / FENCE / RECONFIG that may share the same
	 *	vtable path).
	 */
	if (Tier1Shmem == NULL
		|| Tier1Shmem->peers[target_node_id].state != (int32)CLUSTER_IC_PEER_CONNECTED)
		return CLUSTER_IC_SEND_WOULD_BLOCK; /* HELLO pending; caller retries */

	/*
	 * Hardening v1.0.1 F1 (spec-2.2 v1.0.1 + spec-2.3 v1.0.1 L68):
	 * per-peer outbound buffer for partial writes.  If a previous send
	 * for this peer left bytes pending, drain them first; only attempt
	 * the new caller-supplied buf when the buffer is empty (otherwise
	 * we'd corrupt the byte stream by interleaving frames).
	 *
	 * Three-state return contract:
	 *   DONE        -> full send;counter advance OK
	 *   WOULD_BLOCK -> EAGAIN or partial;outbound buffer holds tail;
	 *                  caller MUST register WL_SOCKET_WRITEABLE for fd
	 *                  and re-enter on writability;NEVER close peer
	 *   HARD_ERROR  -> socket dead;caller MUST close peer
	 */
	if (tier1_outbound_remaining[target_node_id] > 0) {
		int rem = tier1_outbound_remaining[target_node_id];
		int off = tier1_outbound_buf_dyn_size[target_node_id] - rem;
		ssize_t drained;

		Assert(tier1_outbound_buf_dyn[target_node_id] != NULL);

		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_SEND);
		drained = send(fd, &tier1_outbound_buf_dyn[target_node_id][off], (size_t)rem, 0);
		pgstat_report_wait_end();
		if (drained < 0) {
			int saved = errno;

			if (saved == EAGAIN || saved == EWOULDBLOCK)
				return CLUSTER_IC_SEND_WOULD_BLOCK; /* still backpressured */
			peer_record_error(target_node_id, saved, "08006", "send (drain): %s", strerror(saved));
			return CLUSTER_IC_SEND_HARD_ERROR; /* caller closes peer */
		}
		tier1_outbound_remaining[target_node_id] -= (int)drained;
		if (Tier1Shmem != NULL && drained > 0) {
			pg_atomic_add_fetch_u64(&Tier1Shmem->peers[target_node_id].bytes_send, (uint64)drained);
			Tier1Shmem->peers[target_node_id].last_send_at = GetCurrentTimestamp();
		}
		if (tier1_outbound_remaining[target_node_id] > 0)
			return CLUSTER_IC_SEND_WOULD_BLOCK; /* still pending, defer new payload */
	}

	/*
	 * Nonblocking write of the new caller-supplied buf.  Short write
	 * is buffered (F2 dynamic) -- WOULD_BLOCK return tells caller to
	 * drain on next WL_SOCKET_WRITEABLE via the path above.
	 */
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_SEND);
	sent = send(fd, buf, len, 0);
	pgstat_report_wait_end();
	if (sent < 0) {
		int saved_errno = errno;

		if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK) {
			/*
			 * spec-2.5 hardening v1.0.2 F3 (L85 initial-EAGAIN-must-
			 * queue-full-frame): initial 0-byte EAGAIN must queue the
			 * full frame to outbound_buf_dyn so partial-IO drain path
			 * (lines 488-513 above) re-attempts on next WRITEABLE.
			 * Pre-fix code returned WOULD_BLOCK without queueing →
			 * frame silently lost.  CSSD heartbeat happened to be
			 * idempotent (next tick re-sends), but spec-2.13 GES
			 * request / spec-2.18 SI invalidate / spec-2.27 sinval
			 * ack are non-idempotent → frame loss = operation lost.
			 *
			 * Reuse the partial-write tail queue path (treat sent=0
			 * as "tail_len = len" partial write).
			 */
			if (len > PGRAC_IC_PAYLOAD_MAX) {
				peer_record_error(target_node_id, 0, "08006",
								  "initial EAGAIN frame %zu > 16 MB hard cap", len);
				return CLUSTER_IC_SEND_HARD_ERROR;
			}
			if (tier1_outbound_buf_dyn[target_node_id] == NULL
				|| tier1_outbound_buf_dyn_size[target_node_id] < (int)len) {
				MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);

				if (tier1_outbound_buf_dyn[target_node_id] != NULL)
					pfree(tier1_outbound_buf_dyn[target_node_id]);
				tier1_outbound_buf_dyn[target_node_id] = palloc((Size)len);
				tier1_outbound_buf_dyn_size[target_node_id] = (int)len;
				MemoryContextSwitchTo(oldctx);
			}
			memcpy(tier1_outbound_buf_dyn[target_node_id], buf, len);
			tier1_outbound_remaining[target_node_id] = (int)len;

			return CLUSTER_IC_SEND_WOULD_BLOCK;
		}

		/* Hard error -- caller closes peer. */
		peer_record_error(target_node_id, saved_errno, "08006", "send: %s", strerror(saved_errno));
		return CLUSTER_IC_SEND_HARD_ERROR;
	}

	if ((size_t)sent != len) {
		/*
		 * Partial write -- buffer the unsent tail in tier1_outbound_buf_dyn
		 * (per-peer dynamic palloc) so we can complete it on next WRITEABLE.
		 * Frame is message-aligned; we MUST complete it before the next
		 * frame to avoid interleaving payloads on the wire.
		 *
		 * spec-2.4 hardening v1.0.1 F2: dynamic buffer grows lazily up to
		 * PGRAC_IC_PAYLOAD_MAX (16 MB).  Replaces spec-2.2 v1.0.1 static
		 * 36-byte buffer that capped chunk frames at HARD_ERROR.
		 */
		size_t tail_len = len - (size_t)sent;

		if (tail_len > PGRAC_IC_PAYLOAD_MAX) {
			peer_record_error(target_node_id, 0, "08006", "partial send tail %zu > 16 MB hard cap",
							  tail_len);
			return CLUSTER_IC_SEND_HARD_ERROR;
		}

		/* Lazy grow per-peer buffer. */
		if (tier1_outbound_buf_dyn[target_node_id] == NULL
			|| tier1_outbound_buf_dyn_size[target_node_id] < (int)tail_len) {
			MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);

			if (tier1_outbound_buf_dyn[target_node_id] != NULL)
				pfree(tier1_outbound_buf_dyn[target_node_id]);
			tier1_outbound_buf_dyn[target_node_id] = palloc((Size)tail_len);
			tier1_outbound_buf_dyn_size[target_node_id] = (int)tail_len;
			MemoryContextSwitchTo(oldctx);
		}
		memcpy(tier1_outbound_buf_dyn[target_node_id], (const char *)buf + sent, tail_len);
		tier1_outbound_remaining[target_node_id] = (int)tail_len;

		if (Tier1Shmem != NULL && sent > 0) {
			pg_atomic_add_fetch_u64(&Tier1Shmem->peers[target_node_id].bytes_send, (uint64)sent);
			Tier1Shmem->peers[target_node_id].last_send_at = GetCurrentTimestamp();
		}
		/*
		 * WOULD_BLOCK lets caller (LMON) keep peer state intact + register
		 * WL_SOCKET_WRITEABLE; tail will drain on next entry.  Heartbeat
		 * counter is NOT bumped here -- the caller bumps only on DONE.
		 */
		return CLUSTER_IC_SEND_WOULD_BLOCK;
	}

	if (Tier1Shmem != NULL) {
		pg_atomic_add_fetch_u64(&Tier1Shmem->peers[target_node_id].bytes_send, len);
		Tier1Shmem->peers[target_node_id].last_send_at = GetCurrentTimestamp();
	}
	return CLUSTER_IC_SEND_DONE;
}

/*
 * spec-2.3 hardening v1.0.1 F1: pending-outbound accessor for LMON.
 * Returns true iff this peer has bytes queued in tier1_outbound_buf
 * waiting for WL_SOCKET_WRITEABLE.  Used by LMON to decide whether
 * to add WRITEABLE interest to the WaitEventSet for that fd.
 */
bool
cluster_ic_tier1_pending_outbound(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;
	return tier1_outbound_remaining[peer_id] > 0;
}

/* ============================================================
 * spec-2.4 D10 per-peer counter bumpers.
 * Range-checked;noop on out-of-range or shmem-not-initialized.
 * ============================================================ */

void
cluster_ic_tier1_bump_stale_epoch_drop(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return;
	pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].stale_epoch_drop_count, 1);
}

void
cluster_ic_tier1_bump_lamport_advance(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return;
	pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].lamport_observe_advance_count, 1);
}

void
cluster_ic_tier1_bump_chunk_reassembly_timeout(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return;
	pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].chunk_reassembly_timeout_count, 1);
}

/*
 * spec-2.29 D20: hostile-spoof defense.  Bumped when envelope verify
 * sees env_epoch - my_epoch > CLUSTER_EPOCH_OBSERVE_MAX_JUMP (16) — the
 * frame is DROP_NO_CLOSE'd to deny unbounded epoch advance via spoofed
 * envelopes.  Visible via pg_stat_cluster_ic_peer (Step 3 SRF).
 */
void
cluster_ic_tier1_bump_unreasonable_epoch_jump(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return;
	pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].unreasonable_epoch_jump_count, 1);
}

/*
 * spec-2.29 D18b: Bumped when envelope verify sees env_epoch > my_epoch
 * (within MAX_JUMP) AND cluster_epoch_observe_remote CAS-advanced
 * my_epoch.  Cross-instance epoch convergence visibility — peers
 * actively dragging this instance forward via Lamport piggyback.
 */
void
cluster_ic_tier1_bump_epoch_observe_advance(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return;
	pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].epoch_observe_advance_count, 1);
}

void
cluster_ic_tier1_set_chunk_reassembly_active(int32 peer_id, uint32 active)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return;
	pg_atomic_write_u32(&Tier1Shmem->peers[peer_id].chunk_reassembly_active, active);
}

/*
 * spec-2.4 hardening v1.0.1 F3 (L74 cross-aux-process-close-must-be-LMON-mediated):
 * non-LMON callers (chunk timeout, future CSSD / GES backend) request a peer
 * close.  LMON main tick drains the request via cluster_ic_tier1_lmon_drain_close_requests.
 *
 * Reason is logged immediately by the requester (not stored in shmem) so we
 * don't need a per-peer reason buffer.  Idempotent;repeated requests collapse
 * into one close at LMON tick time.
 */
void
cluster_ic_tier1_request_close_peer(int32 peer_id, const char *reason)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return;
	ereport(LOG, (errmsg("cluster_ic tier1 close requested for peer %d: %s", peer_id,
						 reason ? reason : "(no reason)")));
	pg_atomic_write_u32(&Tier1Shmem->peers[peer_id].close_requested, 1);
}

/*
 * spec-2.4 hardening v1.0.1 F3: LMON main tick scans peers for
 * close_requested = 1, performs the actual close.  Caller (LMON) is
 * expected to update lmon_peer_track[peer].fd / substate + wes_dirty
 * for each closed peer based on this function's return value.
 *
 * Returns true if any close was performed.
 *
 * NOTE:caller MUST be LMON main tick context;this function does NOT
 * itself update lmon_peer_track (that's caller-specific knowledge).
 * LMON callers should:
 *
 *   if (cluster_ic_tier1_lmon_drain_close_requests()) {
 *       for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
 *           if (lmon_peer_track[peer].fd != tier1_peer_fds[peer]) {
 *               lmon_peer_track[peer].fd = -1;
 *               lmon_peer_track[peer].substate = LMON_SUB_DOWN;
 *           }
 *       }
 *       wes_dirty = true;
 *   }
 *
 * Or alternatively:LMON main loop can re-read tier1_peer_fds[peer]
 * after each tick and detect fd transitions to -1 to know which
 * peers were closed.
 */
bool
cluster_ic_tier1_lmon_drain_close_requests(void)
{
	int peer;
	bool any_closed = false;

	if (Tier1Shmem == NULL)
		return false;

	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		uint32 req = pg_atomic_read_u32(&Tier1Shmem->peers[peer].close_requested);

		if (req != 0) {
			pg_atomic_write_u32(&Tier1Shmem->peers[peer].close_requested, 0);
			cluster_ic_tier1_close_peer(peer, "LMON-mediated close (deferred)");
			any_closed = true;
		}
	}
	return any_closed;
}

static bool
tier1_peek_sender(int32 *out_sender_node_id)
{
	fd_set rfds;
	struct timeval tv = { 0, 0 };
	int max_fd = -1;
	int i;
	int ready;

	peer_fds_lazy_init();

	FD_ZERO(&rfds);
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		int fd = tier1_peer_fds[i];

		if (fd >= 0) {
			FD_SET(fd, &rfds);
			if (fd > max_fd)
				max_fd = fd;
		}
	}

	if (max_fd < 0)
		return false; /* no connections -- never reached in Step 7 normal flow */

	ready = select(max_fd + 1, &rfds, NULL, NULL, &tv);
	if (ready <= 0)
		return false; /* nothing ready / interrupted */

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		int fd = tier1_peer_fds[i];

		if (fd >= 0 && FD_ISSET(fd, &rfds)) {
			if (out_sender_node_id != NULL)
				*out_sender_node_id = (int32)i;
			return true;
		}
	}
	return false;
}

static bool
tier1_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize, size_t *out_received_len)
{
	int32 sender = -1;
	int fd;
	ssize_t got;

	peer_fds_lazy_init();

	if (out_received_len != NULL)
		*out_received_len = 0;
	if (out_sender_node_id != NULL)
		*out_sender_node_id = -1;

	if (!tier1_peek_sender(&sender))
		return true; /* strict: no data => true with received=0 */

	Assert(sender >= 0 && sender < CLUSTER_MAX_NODES);
	fd = tier1_peer_fds[sender];
	if (fd < 0)
		return true; /* race: closed between peek and now */

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_RECV);
	got = recv(fd, buf, bufsize, 0);
	pgstat_report_wait_end();
	if (got < 0) {
		int saved_errno = errno;

		if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK)
			return true; /* race after peek; treat as no data */

		/* Hard error per spec-2.2 D2 strict bool semantics. */
		peer_record_error(sender, saved_errno, "08006", "recv: %s", strerror(saved_errno));
		cluster_ic_tier1_close_peer(sender, "recv error");
		return false;
	}
	if (got == 0) {
		/* Peer closed connection cleanly -- treat as hard error so
		 * recv_exact loop propagates and LMON drops the peer state. */
		peer_record_error(sender, 0, "08006", "peer closed connection");
		cluster_ic_tier1_close_peer(sender, "peer EOF");
		return false;
	}

	if (out_sender_node_id != NULL)
		*out_sender_node_id = sender;
	if (out_received_len != NULL)
		*out_received_len = (size_t)got;

	if (Tier1Shmem != NULL) {
		pg_atomic_add_fetch_u64(&Tier1Shmem->peers[sender].bytes_recv, (uint64)got);
		Tier1Shmem->peers[sender].last_recv_at = GetCurrentTimestamp();
	}
	return true;
}

static void
tier1_tier_init(void)
{
	peer_fds_lazy_init();

	/*
	 * Listener bind happens in LMON main entry (cluster_ic_tier1_listener_bind),
	 * not here.  cluster_ic_init runs in cluster_init_shmem which is BEFORE
	 * the LMON aux process forks; binding here would tie the listener fd
	 * to the postmaster, not LMON.  Step 7 (D5+D6) calls
	 * cluster_ic_tier1_listener_bind from LmonMain.
	 */
	ereport(LOG, (errmsg("cluster_ic tier1 vtable bound; listener will bind in LMON main loop")));
}

static void
tier1_tier_shutdown(void)
{
	int i;

	peer_fds_lazy_init();

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (tier1_peer_fds[i] >= 0) {
			(void)close(tier1_peer_fds[i]);
			tier1_peer_fds[i] = -1;
		}
	}

	/*
	 * Hardening v1.0.1 F3: listener fd is process-local; close from
	 * the in-process variable.  Shmem only holds metadata, which we
	 * leave for the next LMON respawn to overwrite (pid + incarnation
	 * bumped in listener_bind).
	 */
	if (tier1_listener_fd >= 0) {
		(void)close(tier1_listener_fd);
		tier1_listener_fd = -1;
	}
}

const ClusterICOps ClusterICOps_Tier1 = {
	.send_bytes = tier1_send_bytes,
	.recv_bytes = tier1_recv_bytes,
	.peek_sender = tier1_peek_sender,
	.tier_init = tier1_tier_init,
	.tier_shutdown = tier1_tier_shutdown,
	.tier_name = "tier1",
};


/* ============================================================
 * LMON-internal API.
 * ============================================================ */

int
cluster_ic_tier1_listener_bind(void)
{
	const ClusterNodeInfo *self;
	char self_host[CLUSTER_IC_TIER1_ADDR_LEN];
	int self_port;
	int fd;
	int yes = 1;
	struct sockaddr_in sa;

	peer_fds_lazy_init();

	if (Tier1Shmem == NULL)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_ic tier1 shmem not initialised before listener_bind")));

	/*
	 * Hardening v1.0.1 F3: re-entry within the SAME LMON process is
	 * idempotent (same fd returned).  Across-process must always open
	 * a fresh fd: the integer in shmem from a previous LMON has no
	 * meaning here -- after that LMON crashed the kernel reaped its
	 * fd, and reusing the integer would either land on an unrelated
	 * fd in this process (silent listener failure) or simply be -1.
	 * The previous shmem-stored listener_fd is therefore IGNORED.
	 */
	if (tier1_listener_fd >= 0)
		return tier1_listener_fd;

	self = cluster_conf_lookup_node(cluster_node_id);
	if (self == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("cluster_ic tier1: cluster.node_id=%d not in pgrac.conf", cluster_node_id),
				 errhint("Add a [node.%d] section to pgrac.conf with "
						 "interconnect_addr.",
						 cluster_node_id)));

	if (!parse_host_port(self->interconnect_addr, self_host, sizeof(self_host), &self_port))
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("cluster_ic tier1: cannot parse interconnect_addr \"%s\"",
							   self->interconnect_addr)));

	fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_socket_access(), errmsg("cluster_ic tier1: socket() failed: %m")));

	(void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
	(void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

	if (set_socket_nonblocking(fd) < 0) {
		int saved = errno;

		(void)close(fd);
		errno = saved;
		ereport(FATAL, (errcode_for_socket_access(),
						errmsg("cluster_ic tier1: fcntl O_NONBLOCK failed: %m")));
	}

	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_port = htons((unsigned short)self_port);
	if (inet_pton(AF_INET, self_host, &sa.sin_addr) != 1) {
		(void)close(fd);
		ereport(FATAL, (errcode(ERRCODE_CONFIG_FILE_ERROR),
						errmsg("cluster_ic tier1: inet_pton failed for \"%s\"", self_host),
						errhint("Stage 2 spec-2.2 supports IPv4 dotted-quad only; "
								"IPv6 [::] forms land in a future spec.")));
	}

	if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) < 0) {
		int saved = errno;

		(void)close(fd);
		errno = saved;
		/*
		 * spec-2.2 §3.10: listener bind failure is the ONLY transport-
		 * setup failure that escalates to FATAL; LMON cannot do its job
		 * without a listener.
		 */
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1: bind on %s:%d failed: %m", self_host, self_port)));
	}

	if (listen(fd, /* backlog */ 16) < 0) {
		int saved = errno;

		(void)close(fd);
		errno = saved;
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1: listen on %s:%d failed: %m", self_host, self_port)));
	}

	/*
	 * Hardening v1.0.1 F3: store fd in process-local; record metadata
	 * (port + owner pid + incarnation) in shmem for diagnostic views.
	 * Bumping incarnation lets observers detect "this LMON has
	 * respawned" without trusting stale fd values.
	 */
	tier1_listener_fd = fd;
	Tier1Shmem->listener_port = self_port;
	Tier1Shmem->listener_pid = MyProcPid;
	Tier1Shmem->listener_incarnation++;

	ereport(LOG,
			(errmsg("cluster_ic tier1 listener bound on %s:%d (pid=%d incarnation=%lu)", self_host,
					self_port, (int)MyProcPid, (unsigned long)Tier1Shmem->listener_incarnation)));
	return fd;
}

bool
cluster_ic_tier1_accept_one(int *out_peer_fd, int32 *out_peer_id)
{
	int listener_fd;
	int cfd;
	struct sockaddr_in ca;
	socklen_t clen = sizeof(ca);

	peer_fds_lazy_init();

	if (out_peer_fd != NULL)
		*out_peer_fd = -1;
	if (out_peer_id != NULL)
		*out_peer_id = -1;

	listener_fd = cluster_ic_tier1_get_listener_fd();
	if (listener_fd < 0)
		return false;

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_ACCEPT);
	cfd = accept(listener_fd, (struct sockaddr *)&ca, &clen);
	pgstat_report_wait_end();
	if (cfd < 0) {
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return false; /* no pending connection */
		ereport(WARNING, (errcode_for_socket_access(), errmsg("cluster_ic tier1 accept(): %m")));
		return false;
	}

	if (set_socket_nonblocking(cfd) < 0) {
		(void)close(cfd);
		return false;
	}

	/* spec-2.4 D8 TCP KeepAlive (per-peer kernel-level half-open detection). */
	apply_tcp_keepalive(cfd, "passive accept fd");

	/*
	 * Peer identity is unknown until we recv + verify HELLO.  Step 7
	 * LMON main loop will register this fd in WaitEventSet and call
	 * cluster_ic_tier1_recv_and_verify_hello(unknown_peer, cfd) once
	 * readable; on success peer_id is learned from HELLO.
	 *
	 * For Step 6 callers (tests / standalone sanity), we return the
	 * fd with peer_id = -1 indicating "HELLO pending".
	 */
	if (out_peer_fd != NULL)
		*out_peer_fd = cfd;
	if (out_peer_id != NULL)
		*out_peer_id = -1;
	return true;
}

bool
cluster_ic_tier1_connect_one(int32 peer_id, int *out_peer_fd)
{
	const char *addr;
	char host[CLUSTER_IC_TIER1_ADDR_LEN];
	int port;
	int fd;
	int yes = 1;
	struct sockaddr_in sa;
	int rc;

	peer_fds_lazy_init();

	if (out_peer_fd != NULL)
		*out_peer_fd = -1;

	addr = peer_addr(peer_id);
	if (addr == NULL) {
		peer_record_error(peer_id, 0, "08001", "peer not declared in pgrac.conf");
		return false;
	}
	if (!parse_host_port(addr, host, sizeof(host), &port)) {
		peer_record_error(peer_id, 0, "08001", "bad interconnect_addr \"%s\"", addr);
		return false;
	}

	fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd < 0) {
		int saved = errno;

		peer_record_error(peer_id, saved, "08001", "socket: %s", strerror(saved));
		return false;
	}

	(void)setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
	if (set_socket_nonblocking(fd) < 0) {
		int saved = errno;

		(void)close(fd);
		peer_record_error(peer_id, saved, "08001", "fcntl O_NONBLOCK: %s", strerror(saved));
		return false;
	}

	/* spec-2.4 D8 TCP KeepAlive on active connect fd. */
	{
		char label[32];

		snprintf(label, sizeof(label), "active connect peer %d", peer_id);
		apply_tcp_keepalive(fd, label);
	}

	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_port = htons((unsigned short)port);
	if (inet_pton(AF_INET, host, &sa.sin_addr) != 1) {
		(void)close(fd);
		peer_record_error(peer_id, 0, "08001", "inet_pton failed for \"%s\"", host);
		return false;
	}

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_CONNECT);
	rc = connect(fd, (struct sockaddr *)&sa, sizeof(sa));
	pgstat_report_wait_end();
	if (rc < 0 && errno != EINPROGRESS) {
		int saved = errno;

		(void)close(fd);
		peer_record_error(peer_id, saved, "08001", "connect %s:%d: %s", host, port,
						  strerror(saved));
		Tier1Shmem->peers[peer_id].connect_error_count++;
		return false;
	}

	tier1_peer_fds[peer_id] = fd;
	Tier1Shmem->peers[peer_id].state = (int32)CLUSTER_IC_PEER_CONNECTING;
	if (out_peer_fd != NULL)
		*out_peer_fd = fd;
	return true;
}

bool
cluster_ic_tier1_finish_connect(int32 peer_id, int peer_fd)
{
	int so_error = 0;
	socklen_t so_error_len = sizeof(so_error);
	const char *self_cluster_name;

	if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &so_error, &so_error_len) < 0 || so_error != 0) {
		int saved = (so_error != 0) ? so_error : errno;

		peer_record_error(peer_id, saved, "08001", "connect SO_ERROR: %s", strerror(saved));
		cluster_ic_tier1_close_peer(peer_id, "connect failed");
		return false;
	}

	/*
	 * Hardening v1.0.1 F1: seed HELLO send buffer + delegate to
	 * continue_hello_send for the actual byte-pushing.  This lets
	 * partial sends (TCP fragmentation on real LANs) recover via
	 * subsequent WL_SOCKET_WRITEABLE wakeups without losing the
	 * already-sent prefix.
	 */
	self_cluster_name = (ClusterConfShmem != NULL) ? ClusterConfShmem->cluster_name : "";
	cluster_ic_build_hello(tier1_hello_send_buf[peer_id], PGRAC_IC_HELLO_VERSION_V1,
						   PGRAC_IC_ENVELOPE_VERSION_V1, cluster_node_id, self_cluster_name);
	tier1_hello_send_remaining[peer_id] = PGRAC_IC_HELLO_BYTES;

	return cluster_ic_tier1_continue_hello_send(peer_id, peer_fd);
}

/*
 * Hardening v1.0.1 F1: continue an in-progress HELLO send.  Caller
 * (LMON) invokes this from finish_connect AND on each WL_SOCKET_WRITEABLE
 * wakeup until tier1_hello_send_remaining[peer_id] reaches 0; at that
 * point the active side flips peer state to CONNECTED.
 *
 * Return semantics:
 *   true  + remaining > 0 = partial send; LMON keeps WL_SOCKET_WRITEABLE
 *   true  + remaining = 0 = HELLO complete; LMON should switch to READABLE
 *   false                  = hard error; LMON should close + DOWN
 */
bool
cluster_ic_tier1_continue_hello_send(int32 peer_id, int peer_fd)
{
	int rem;
	int off;
	ssize_t sent;

	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return false;
	if (peer_fd < 0)
		return false;

	rem = tier1_hello_send_remaining[peer_id];
	if (rem <= 0)
		return true; /* nothing to do (already complete) */

	off = PGRAC_IC_HELLO_BYTES - rem;
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_SEND);
	sent = send(peer_fd, &tier1_hello_send_buf[peer_id][off], (size_t)rem, 0);
	pgstat_report_wait_end();

	if (sent < 0) {
		int saved = errno;

		if (saved == EAGAIN || saved == EWOULDBLOCK)
			return true; /* will retry on next WRITEABLE */

		peer_record_error(peer_id, saved, "08006", "HELLO send: %s", strerror(saved));
		cluster_ic_tier1_close_peer(peer_id, "HELLO send error");
		return false;
	}

	tier1_hello_send_remaining[peer_id] -= (int)sent;

	if (tier1_hello_send_remaining[peer_id] > 0)
		return true; /* partial; LMON re-enters on next WRITEABLE */

	/*
	 * spec-2.2 §2.4 -- HELLO fully sent; active considers itself
	 * CONNECTED.  Passive verifier reads the HELLO and either
	 * CONNECTEDs or rejects + closes (active detects rejection on
	 * next heartbeat send/recv).  No HELLO_ACK.
	 */
	if (Tier1Shmem != NULL) {
		Tier1Shmem->peers[peer_id].state = (int32)CLUSTER_IC_PEER_CONNECTED;
		Tier1Shmem->peers[peer_id].last_connect_at = GetCurrentTimestamp();
	}
	ereport(LOG,
			(errmsg("cluster_ic tier1 peer %d HELLO sent, state CONNECTED (active)", peer_id)));
	return true;
}

bool
cluster_ic_tier1_recv_and_verify_hello(int32 peer_id, int peer_fd)
{
	uint8 hello_buf[PGRAC_IC_HELLO_BYTES];
	ssize_t got;
	ClusterICHelloMsg msg;
	const char *self_cluster_name;
	const ClusterNodeInfo *peer_info;

	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_RECV);
	got = recv(peer_fd, hello_buf, PGRAC_IC_HELLO_BYTES, MSG_WAITALL);
	pgstat_report_wait_end();
	if (got != PGRAC_IC_HELLO_BYTES) {
		int saved = (got < 0) ? errno : 0;

		peer_record_error(peer_id, saved, "08P01", "HELLO recv short or failed (%zd of %d): %s",
						  got, PGRAC_IC_HELLO_BYTES, saved ? strerror(saved) : "short read");
		cluster_ic_tier1_close_peer(peer_id, "HELLO recv failed");
		return false;
	}

	if (!cluster_ic_parse_hello(hello_buf, &msg)) {
		peer_record_error(peer_id, 0, "08P01", "HELLO bad magic");
		cluster_ic_tier1_close_peer(peer_id, "HELLO bad magic");
		Tier1Shmem->peers[peer_id].state = (int32)CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	if (msg.hello_version != PGRAC_IC_HELLO_VERSION_V1
		|| msg.envelope_version != PGRAC_IC_ENVELOPE_VERSION_V1) {
		peer_record_error(peer_id, 0, "08P01", "HELLO version mismatch (hello=%u env=%u)",
						  msg.hello_version, msg.envelope_version);
		cluster_ic_tier1_close_peer(peer_id, "HELLO version mismatch");
		Tier1Shmem->peers[peer_id].state = (int32)CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	self_cluster_name = (ClusterConfShmem != NULL) ? ClusterConfShmem->cluster_name : "";
	if (ClusterConfShmem != NULL && strcmp(msg.cluster_name, self_cluster_name) != 0) {
		peer_record_error(peer_id, 0, "08P01",
						  "HELLO cluster_name mismatch (peer=\"%s\" mine=\"%s\")", msg.cluster_name,
						  self_cluster_name);
		cluster_ic_tier1_close_peer(peer_id, "HELLO cluster_name mismatch");
		Tier1Shmem->peers[peer_id].state = (int32)CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	peer_info = cluster_conf_lookup_node(msg.source_node_id);
	if (peer_info == NULL) {
		peer_record_error(peer_id, 0, "08P01", "HELLO unknown source_node_id %d",
						  msg.source_node_id);
		cluster_ic_tier1_close_peer(peer_id, "HELLO unknown peer");
		Tier1Shmem->peers[peer_id].state = (int32)CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	if (peer_id >= 0 && peer_id != msg.source_node_id) {
		peer_record_error(peer_id, 0, "08P01", "HELLO source_node_id %d != expected %d",
						  msg.source_node_id, peer_id);
		cluster_ic_tier1_close_peer(peer_id, "HELLO peer id mismatch");
		Tier1Shmem->peers[peer_id].state = (int32)CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	/* On accept side peer_id was -1 until now; bind fd to learned peer. */
	if (peer_id < 0) {
		peer_id = msg.source_node_id;
		tier1_peer_fds[peer_id] = peer_fd;
	}

	Tier1Shmem->peers[peer_id].state = (int32)CLUSTER_IC_PEER_CONNECTED;
	Tier1Shmem->peers[peer_id].last_connect_at = GetCurrentTimestamp();
	(void)peer_addr(peer_id); /* cache addr in shmem for view */

	ereport(LOG, (errmsg("cluster_ic tier1 peer %d HELLO verified, state CONNECTED", peer_id)));
	return true;
}

ClusterICSendResult
cluster_ic_tier1_send_heartbeat(int32 peer_id)
{
	ClusterICSendResult rc;

	/*
	 * spec-2.3 D5 wire format -- HEARTBEAT is a 36-byte ClusterICEnvelope
	 * with msg_type = PGRAC_IC_MSG_HEARTBEAT and payload_len = 0.
	 * cluster_ic_send_envelope (cluster_ic_router.c) does the producer-
	 * mask check (LMON-only per §3.4 + spec-2.2 §3.9 升级), envelope
	 * build + CRC, then delegates to cluster_ic_send_bytes (vtable) →
	 * tier1_send_bytes which honors v1.0.1 F1 partial-IO buffer for
	 * short-write recovery.
	 *
	 * Per §3.6 boundary invariant: heartbeat carries IC transport
	 * liveness only -- it does NOT trigger fence / membership / quorum.
	 *
	 * spec-2.3 hardening v1.0.1 F1 (L68): three-state return.  Caller
	 * (LMON main loop) MUST switch on result -- WOULD_BLOCK means the
	 * outbound buffer holds the tail and LMON should register
	 * WL_SOCKET_WRITEABLE for fd, NOT close peer.
	 */
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return CLUSTER_IC_SEND_HARD_ERROR;
	if (Tier1Shmem->peers[peer_id].state != (int32)CLUSTER_IC_PEER_CONNECTED)
		return CLUSTER_IC_SEND_HARD_ERROR;
	if (tier1_peer_fds[peer_id] < 0)
		return CLUSTER_IC_SEND_HARD_ERROR;

	rc = cluster_ic_send_envelope(PGRAC_IC_MSG_HEARTBEAT, peer_id, NULL, 0);
	if (rc != CLUSTER_IC_SEND_DONE)
		return rc; /* propagate WOULD_BLOCK / HARD_ERROR */

	pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].heartbeat_send_count, 1);
	Tier1Shmem->peers[peer_id].last_heartbeat_sent_at = GetCurrentTimestamp();
	return CLUSTER_IC_SEND_DONE;
}

/*
 * Hardening v1.0.1 F1: anon-slot HELLO recv accumulator (passive side).
 * See cluster_ic_tier1.h for full contract.  Replaces the single-shot
 * recv_and_verify_hello path that assumed 64-byte HELLO arrived in
 * one segment (broken on real-network TCP fragmentation).
 */
bool
cluster_ic_tier1_continue_hello_recv(int anon_slot, int peer_fd, int32 *out_learned_peer_id)
{
	int len;
	int need;
	ssize_t got;
	ClusterICHelloMsg msg;
	const char *self_cluster_name;
	const ClusterNodeInfo *peer_info;
	int32 learned;

	if (out_learned_peer_id != NULL)
		*out_learned_peer_id = -1;
	if (anon_slot < 0 || anon_slot >= CLUSTER_MAX_NODES)
		return false;
	if (peer_fd < 0)
		return false;

	len = tier1_anon_hello_len[anon_slot];
	need = PGRAC_IC_HELLO_BYTES - len;

	if (need > 0) {
		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_RECV);
		got = recv(peer_fd, &tier1_anon_hello_buf[anon_slot][len], (size_t)need, 0);
		pgstat_report_wait_end();
		if (got < 0) {
			int saved = errno;

			if (saved == EAGAIN || saved == EWOULDBLOCK)
				return true; /* wait next WL_SOCKET_READABLE */
			ereport(LOG, (errmsg("cluster_ic tier1 HELLO recv error on anon slot %d: %s", anon_slot,
								 strerror(saved))));
			return false;
		}
		if (got == 0) {
			ereport(LOG,
					(errmsg("cluster_ic tier1 HELLO recv: peer EOF on anon slot %d", anon_slot)));
			return false;
		}
		tier1_anon_hello_len[anon_slot] += (int)got;

		if (tier1_anon_hello_len[anon_slot] < PGRAC_IC_HELLO_BYTES)
			return true; /* partial; wait next READABLE */
	}

	/* Full HELLO assembled; parse + verify. */
	if (!cluster_ic_parse_hello(tier1_anon_hello_buf[anon_slot], &msg)) {
		/*
		 * spec-2.13 Hardening v1.0.2 diagnostic instrumentation (L66-family
		 * root-cause investigation):  when HELLO parse fails, dump the
		 * actual wire bytes + connection metadata so we can distinguish
		 * (a) PG startup-packet串线 / (b) HTTP probe / (c) old HELLO
		 * version / (d) partial-recv accumulator bug / (e) memory
		 * corruption / (f) other process串线.  Remove after root cause
		 * pinpointed.
		 */
		{
			struct sockaddr_in peer_sa;
			socklen_t peer_sa_len = sizeof(peer_sa);
			char peer_ip[INET_ADDRSTRLEN] = "?";
			int peer_port = -1;
			const uint8 *b = tier1_anon_hello_buf[anon_slot];
			const char *self_name
				= (ClusterConfShmem != NULL) ? ClusterConfShmem->cluster_name : "(no-conf)";

			if (getpeername(peer_fd, (struct sockaddr *)&peer_sa, &peer_sa_len) == 0) {
				if (inet_ntop(AF_INET, &peer_sa.sin_addr, peer_ip, sizeof(peer_ip)) == NULL)
					strcpy(peer_ip, "?");
				peer_port = ntohs(peer_sa.sin_port);
			}
			ereport(LOG,
					(errmsg("cluster_ic tier1 HELLO bad magic on anon slot %d "
							"(fd=%d peer=%s:%d cluster_name=\"%s\" accum_len=%d "
							"hex[0..15]=%02x %02x %02x %02x %02x %02x %02x %02x "
							"%02x %02x %02x %02x %02x %02x %02x %02x)",
							anon_slot, peer_fd, peer_ip, peer_port, self_name,
							tier1_anon_hello_len[anon_slot], b[0], b[1], b[2], b[3], b[4], b[5],
							b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15])));
		}
		return false;
	}

	if (msg.hello_version != PGRAC_IC_HELLO_VERSION_V1
		|| msg.envelope_version != PGRAC_IC_ENVELOPE_VERSION_V1) {
		ereport(LOG, (errmsg("cluster_ic tier1 HELLO version mismatch (hello=%u env=%u)",
							 msg.hello_version, msg.envelope_version)));
		return false;
	}

	self_cluster_name = (ClusterConfShmem != NULL) ? ClusterConfShmem->cluster_name : "";
	if (ClusterConfShmem != NULL && strcmp(msg.cluster_name, self_cluster_name) != 0) {
		ereport(LOG,
				(errmsg("cluster_ic tier1 HELLO cluster_name mismatch (peer=\"%s\" mine=\"%s\")",
						msg.cluster_name, self_cluster_name)));
		return false;
	}

	peer_info = cluster_conf_lookup_node(msg.source_node_id);
	if (peer_info == NULL) {
		ereport(LOG,
				(errmsg("cluster_ic tier1 HELLO unknown source_node_id %d", msg.source_node_id)));
		return false;
	}

	/* Bind learned peer_id; record state CONNECTED. */
	learned = msg.source_node_id;
	tier1_peer_fds[learned] = peer_fd;
	if (Tier1Shmem != NULL) {
		peer_record_error(learned, 0, "", ""); /* clear any prior */
		Tier1Shmem->peers[learned].state = (int32)CLUSTER_IC_PEER_CONNECTED;
		Tier1Shmem->peers[learned].last_connect_at = GetCurrentTimestamp();
		(void)peer_addr(learned);
	}

	if (out_learned_peer_id != NULL)
		*out_learned_peer_id = learned;

	ereport(LOG, (errmsg("cluster_ic tier1 anon slot %d HELLO verified -> peer %d state CONNECTED",
						 anon_slot, learned)));
	return true;
}

void
cluster_ic_tier1_anon_hello_reset(int anon_slot)
{
	if (anon_slot < 0 || anon_slot >= CLUSTER_MAX_NODES)
		return;
	tier1_anon_hello_len[anon_slot] = 0;
}

int
cluster_ic_tier1_hello_send_remaining(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return 0;
	return tier1_hello_send_remaining[peer_id];
}

/*
 * spec-2.3 D6: drain pending envelopes from peer_fd.
 *
 *   Called by LMON when per-peer fd is WL_SOCKET_READABLE in CONNECTED
 *   state.  Reads non-blocking until EAGAIN, accumulating bytes into
 *   tier1_recv_buf[N][36] (per-peer 36-byte buffer).  For each complete
 *   ClusterICEnvelope:
 *     1. cluster_ic_envelope_verify (6-step validation: magic / version /
 *        source / dest / payload_length / CRC)
 *     2. cluster_ic_dispatch_envelope (lookup dispatch_table[msg_type] +
 *        invoke registered handler via PG_TRY/CATCH wrap)
 *
 *   For HEARTBEAT specifically, spec-2.3 ships payload_length = 0, so
 *   verify + dispatch consume the 36-byte envelope alone (no separate
 *   payload bytes on the wire).  Future spec-2.4+ adds framing for
 *   non-zero payload msgs; this function will gain a "read N more
 *   bytes for payload" branch then.
 *
 *   Returns false on hard recv error / EOF / verify failure / unregistered
 *   msg_type -- caller (LMON) is expected to close_peer per spec-2.3
 *   §3.5b inbound rule (peer-level failure; NEVER ereport ERROR LMON).
 *   Returns true on EAGAIN (drained for now).
 */
bool
cluster_ic_tier1_recv_heartbeat_drain(int32 peer_id, int peer_fd)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return false;
	if (peer_fd < 0)
		return false;

	for (;;) {
		ssize_t got;

		/*
		 * spec-2.4 hardening v1.0.1 F1 (L76 register-vs-handler-signature-coupling):
		 * Two-phase recv state machine.
		 *   phase 0: read PGRAC_IC_ENVELOPE_BYTES (36 B) into tier1_recv_buf
		 *   phase 1: peek envelope.payload_length;if > 0, lazy palloc
		 *            tier1_recv_payload_buf_dyn[peer] (in TopMemoryContext;
		 *            up to PGRAC_IC_PAYLOAD_MAX); read remaining payload
		 *            bytes; then verify+dispatch with payload + payload_len.
		 *
		 * EAGAIN at any read returns true with state preserved -- LMON
		 * re-enters on next WL_SOCKET_READABLE.
		 */

		if (tier1_recv_phase[peer_id] == 0) {
			/* Phase 0: filling envelope buffer. */
			int buf_len = tier1_recv_buf_len[peer_id];
			int need = PGRAC_IC_ENVELOPE_BYTES - buf_len;

			Assert(need > 0);

			pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_RECV);
			got = recv(peer_fd, &tier1_recv_buf[peer_id][buf_len], (size_t)need, 0);
			pgstat_report_wait_end();
			if (got < 0) {
				int saved = errno;

				if (saved == EAGAIN || saved == EWOULDBLOCK)
					return true; /* drained for now */
				peer_record_error(peer_id, saved, "08006", "envelope recv: %s", strerror(saved));
				return false;
			}
			if (got == 0) {
				peer_record_error(peer_id, 0, "08006", "peer closed connection (envelope drain)");
				return false;
			}

			buf_len += (int)got;
			tier1_recv_buf_len[peer_id] = buf_len;

			if (Tier1Shmem != NULL) {
				pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].bytes_recv, (uint64)got);
				Tier1Shmem->peers[peer_id].last_recv_at = GetCurrentTimestamp();
			}

			if (buf_len < PGRAC_IC_ENVELOPE_BYTES)
				continue; /* partial envelope, keep reading */

			/*
			 * One full 36-byte envelope assembled.  Peek payload_length
			 * and decide whether to enter phase 1.
			 */
			{
				ClusterICEnvelope env_peek;
				uint32 plen;

				memcpy(&env_peek, tier1_recv_buf[peer_id], PGRAC_IC_ENVELOPE_BYTES);
				plen = env_peek.payload_length;

				if (plen > PGRAC_IC_PAYLOAD_MAX) {
					peer_record_error(peer_id, 0, "08P01",
									  "envelope payload_length %u exceeds 16 MB cap "
									  "(msg_type=%u sender=%u)",
									  plen, env_peek.msg_type, env_peek.source_node_id);
					tier1_recv_buf_len[peer_id] = 0;
					return false;
				}

				if (plen == 0) {
					/* No payload -- proceed directly to verify+dispatch
					 * with NULL payload + payload_len=0 (HEARTBEAT path). */
					goto verify_and_dispatch;
				}

				/* Enter phase 1: lazy palloc payload buf in TopMemoryContext.
				 *
				 * spec-2.5 hardening v1.0.1 F3 (L80 dynamic-buffer-must-track-
				 * capacity): grow the per-peer buffer if plen exceeds current
				 * capacity.  Pre-fix code only palloc'd when buf == NULL,
				 * locking capacity to the FIRST frame's payload size and
				 * causing memcpy overrun on subsequent larger frames. */
				if (tier1_recv_payload_buf_dyn[peer_id] == NULL
					|| (int)plen > tier1_recv_payload_buf_dyn_capacity[peer_id]) {
					MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);

					if (tier1_recv_payload_buf_dyn[peer_id] != NULL)
						pfree(tier1_recv_payload_buf_dyn[peer_id]);
					tier1_recv_payload_buf_dyn[peer_id] = palloc((Size)plen);
					tier1_recv_payload_buf_dyn_capacity[peer_id] = (int)plen;
					MemoryContextSwitchTo(oldctx);
				}
				tier1_recv_payload_total[peer_id] = (int)plen;
				tier1_recv_payload_filled[peer_id] = 0;
				tier1_recv_phase[peer_id] = 1;
				/* fall through to phase 1 read in next loop iteration */
				continue;
			}
		} else {
			/* Phase 1: filling payload buffer. */
			int filled = tier1_recv_payload_filled[peer_id];
			int total = tier1_recv_payload_total[peer_id];
			int need = total - filled;

			Assert(need > 0);
			Assert(tier1_recv_payload_buf_dyn[peer_id] != NULL);

			pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_RECV);
			got = recv(peer_fd, tier1_recv_payload_buf_dyn[peer_id] + filled, (size_t)need, 0);
			pgstat_report_wait_end();
			if (got < 0) {
				int saved = errno;

				if (saved == EAGAIN || saved == EWOULDBLOCK)
					return true;
				peer_record_error(peer_id, saved, "08006", "payload recv: %s", strerror(saved));
				return false;
			}
			if (got == 0) {
				peer_record_error(peer_id, 0, "08006", "peer closed connection (payload drain)");
				return false;
			}

			filled += (int)got;
			tier1_recv_payload_filled[peer_id] = filled;

			if (Tier1Shmem != NULL) {
				pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].bytes_recv, (uint64)got);
				Tier1Shmem->peers[peer_id].last_recv_at = GetCurrentTimestamp();
			}

			if (filled < total)
				continue; /* partial payload, keep reading */

			/* Full envelope + payload assembled.  Verify + dispatch. */
			goto verify_and_dispatch;
		}

	verify_and_dispatch: {
		ClusterICEnvelope env;
		const void *payload = NULL;
		uint32 payload_len = 0;

		memcpy(&env, tier1_recv_buf[peer_id], PGRAC_IC_ENVELOPE_BYTES);

		if (tier1_recv_phase[peer_id] == 1) {
			payload = tier1_recv_payload_buf_dyn[peer_id];
			payload_len = (uint32)tier1_recv_payload_total[peer_id];
		}

		/*
		 * spec-2.4 hardening v1.0.1 F4 (L75 verify-tri-state-return):
		 * accept_and_observe returns enum.  Switch on result:
		 *   OK              -> dispatch
		 *   DROP_NO_CLOSE   -> drop frame (LOG already emitted by verify
		 *                      step 7 for stale epoch);reset state, KEEP
		 *                      peer connected
		 *   PEER_FAILURE    -> close peer (peer-level failure)
		 */
		{
			ClusterICEnvelopeVerifyResult vrc = cluster_ic_envelope_accept_and_observe(
				&env, payload, payload_len, (uint32)cluster_node_id, peer_id);

			if (vrc == CLUSTER_IC_ENVELOPE_DROP_NO_CLOSE) {
				/* Drop frame, keep peer.  Reset state for next frame. */
				tier1_recv_buf_len[peer_id] = 0;
				tier1_recv_phase[peer_id] = 0;
				tier1_recv_payload_filled[peer_id] = 0;
				tier1_recv_payload_total[peer_id] = 0;
				continue; /* loop back to read next frame */
			}
			if (vrc == CLUSTER_IC_ENVELOPE_PEER_FAILURE) {
				peer_record_error(peer_id, 0, "08P01",
								  "envelope verify hard failure (magic=0x%x version=%u "
								  "msg_type=%u src=%u dst=%u plen=%u crc=0x%x peer_id=%d)",
								  env.magic, env.version, env.msg_type, env.source_node_id,
								  env.dest_node_id, env.payload_length, env.payload_crc32c,
								  peer_id);
				tier1_recv_buf_len[peer_id] = 0;
				tier1_recv_phase[peer_id] = 0;
				tier1_recv_payload_filled[peer_id] = 0;
				tier1_recv_payload_total[peer_id] = 0;
				return false;
			}
			/* OK -> fall through to dispatch */
		}

		/* HEARTBEAT-specific bookkeeping. */
		if (env.msg_type == PGRAC_IC_MSG_HEARTBEAT) {
			pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].heartbeat_recv_count, 1);
			Tier1Shmem->peers[peer_id].last_heartbeat_recv_at = GetCurrentTimestamp();
		}

		/*
			 * spec-2.4 hardening v1.0.1 F1: dispatch_envelope now takes
			 * peer_id (signature change) so msg_type=255 chunk fast path
			 * can route to chunk_dispatch_frame with caller's known peer.
			 */
		if (!cluster_ic_dispatch_envelope(&env, payload, peer_id)) {
			peer_record_error(peer_id, 0, "08P01",
							  "envelope msg_type %u not registered (sender %u)", env.msg_type,
							  env.source_node_id);
			tier1_recv_buf_len[peer_id] = 0;
			tier1_recv_phase[peer_id] = 0;
			tier1_recv_payload_filled[peer_id] = 0;
			tier1_recv_payload_total[peer_id] = 0;
			return false;
		}

		/* Reset phase state for next frame. */
		tier1_recv_buf_len[peer_id] = 0;
		tier1_recv_phase[peer_id] = 0;
		tier1_recv_payload_filled[peer_id] = 0;
		tier1_recv_payload_total[peer_id] = 0;
		/* loop again; peer may have queued multiple frames */
	}
	}
}

void
cluster_ic_tier1_close_peer(int32 peer_id, const char *reason)
{
	peer_fds_lazy_init();

	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;

	if (tier1_peer_fds[peer_id] >= 0) {
		(void)close(tier1_peer_fds[peer_id]);
		tier1_peer_fds[peer_id] = -1;
	}

	if (Tier1Shmem != NULL) {
		/* Don't overwrite REJECTED state -- that's a stickier verdict. */
		if (Tier1Shmem->peers[peer_id].state != (int32)CLUSTER_IC_PEER_REJECTED)
			Tier1Shmem->peers[peer_id].state = (int32)CLUSTER_IC_PEER_DOWN;
		Tier1Shmem->peers[peer_id].reconnect_count++;
	}

	/*
	 * spec-2.4 D6 -- chunk reassembly state cleanup on peer close.
	 * Idempotent (no-op when peer has no in-flight chunked recv).
	 * Single chunk_reset_peer call atomically frees the per-peer
	 * AllocSetContext + buf + state (per Q4 修订:cleanup correctness
	 * is a property of the design, not of distributed pfree discipline).
	 */
	cluster_ic_chunk_reset_peer(peer_id);

	/*
	 * spec-2.4 hardening v1.0.1 F5 (L73 close-peer-must-clean-all-per-peer
	 * -process-local):reset ALL per-peer process-local state machines.
	 * Without this, reconnect after close inherits stale half-frame state
	 * -> frame stream corruption guaranteed.
	 */
	tier1_recv_buf_len[peer_id] = 0;
	tier1_outbound_remaining[peer_id] = 0;
	tier1_hello_send_remaining[peer_id] = 0;
	tier1_anon_hello_len[peer_id] = 0;

	/* spec-2.4 hardening v1.0.1 F1: variable-length payload phase state. */
	tier1_recv_phase[peer_id] = 0;
	tier1_recv_payload_total[peer_id] = 0;
	tier1_recv_payload_filled[peer_id] = 0;
	if (tier1_recv_payload_buf_dyn[peer_id] != NULL) {
		pfree(tier1_recv_payload_buf_dyn[peer_id]);
		tier1_recv_payload_buf_dyn[peer_id] = NULL;
	}
	tier1_recv_payload_buf_dyn_capacity[peer_id] = 0; /* spec-2.5 v1.0.1 F3 */

	/* spec-2.4 hardening v1.0.1 F2: dynamic outbound buf. */
	if (tier1_outbound_buf_dyn[peer_id] != NULL) {
		pfree(tier1_outbound_buf_dyn[peer_id]);
		tier1_outbound_buf_dyn[peer_id] = NULL;
		tier1_outbound_buf_dyn_size[peer_id] = 0;
	}

	if (reason != NULL)
		ereport(LOG, (errmsg("cluster_ic tier1 peer %d closed: %s", peer_id, reason)));
}

const ClusterICPeerStateShmem *
cluster_ic_tier1_peer_get(int32 peer_id)
{
	if (Tier1Shmem == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return NULL;
	if (cluster_conf_lookup_node(peer_id) == NULL)
		return NULL;
	return &Tier1Shmem->peers[peer_id];
}

int
cluster_ic_tier1_get_listener_fd(void)
{
	/*
	 * Hardening v1.0.1 F3: returns process-local fd.  Valid only inside
	 * the LMON process that bound the listener.  Other processes get
	 * -1 (their tier1_listener_fd static is its own per-process copy
	 * and is never set in non-LMON processes).
	 */
	return tier1_listener_fd;
}

/*
 * Hardening v1.0.1 F3: listener metadata accessors -- shmem-backed
 * so any backend can observe "which LMON owns the listener" and
 * "how many times has it respawned".  Used by cluster_debug.c
 * (pg_cluster_state) for observability + by t/077 TAP for respawn
 * verification.
 */
pid_t
cluster_ic_tier1_get_listener_pid(void)
{
	return Tier1Shmem != NULL ? Tier1Shmem->listener_pid : 0;
}

uint64
cluster_ic_tier1_get_listener_incarnation(void)
{
	return Tier1Shmem != NULL ? Tier1Shmem->listener_incarnation : 0;
}

int
cluster_ic_tier1_get_listener_port(void)
{
	return Tier1Shmem != NULL ? Tier1Shmem->listener_port : -1;
}

int
cluster_ic_tier1_get_peer_fd(int32 peer_id)
{
	peer_fds_lazy_init();
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return -1;
	return tier1_peer_fds[peer_id];
}


/* ============================================================
 * spec-2.2 D9 -- pg_cluster_ic_peers SRF body.
 *
 * Returns one row per peer declared in pgrac.conf (skips slots with
 * node_id == -1 = unconfigured).  19 columns per spec-2.2 §2.6
 * frozen layout.  Per spec-2.2 §3.6 the `state` column reports
 * TRANSPORT-LEVEL liveness only.
 * ============================================================ */

static const char *
peer_state_to_string(int32 s)
{
	switch ((ClusterICPeerState)s) {
	case CLUSTER_IC_PEER_DOWN:
		return "down";
	case CLUSTER_IC_PEER_CONNECTING:
		return "connecting";
	case CLUSTER_IC_PEER_CONNECTED:
		return "connected";
	case CLUSTER_IC_PEER_REJECTED:
		return "rejected";
	}
	return "unknown";
}

Datum
cluster_get_ic_peers(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	int i;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (Tier1Shmem == NULL)
		PG_RETURN_VOID();

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		ClusterICPeerStateShmem *p = &Tier1Shmem->peers[i];
		Datum values[23];
		bool nulls[23];
		int col = 0;

		if (cluster_conf_lookup_node(i) == NULL)
			continue; /* peer not declared in pgrac.conf */

		memset(nulls, false, sizeof(nulls));

		values[col++] = Int32GetDatum(i);
		values[col++] = CStringGetTextDatum(peer_state_to_string(p->state));
		values[col++] = CStringGetTextDatum(p->interconnect_addr[0] ? p->interconnect_addr : "");
#define ADD_TS(field)                                                                              \
	do {                                                                                           \
		if (p->field == 0)                                                                         \
			nulls[col] = true;                                                                     \
		else                                                                                       \
			values[col] = TimestampTzGetDatum(p->field);                                           \
		col++;                                                                                     \
	} while (0)
		ADD_TS(last_connect_at);
		ADD_TS(last_send_at);
		ADD_TS(last_recv_at);
		ADD_TS(last_heartbeat_sent_at);
		ADD_TS(last_heartbeat_recv_at);
#undef ADD_TS
		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->heartbeat_send_count));
		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->heartbeat_recv_count));
		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->msg_send_count));
		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->msg_recv_count));
		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->bytes_send));
		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->bytes_recv));
		values[col++] = Int32GetDatum((int32)p->reconnect_count);
		values[col++] = Int32GetDatum((int32)p->connect_error_count);
		values[col++] = Int32GetDatum(p->last_errno);
		values[col++] = CStringGetTextDatum(p->last_error_code[0] ? p->last_error_code : "");
		values[col++] = CStringGetTextDatum(p->last_error[0] ? p->last_error : "");
		/* spec-2.4 D11: 4 NEW columns (19 -> 23). */
		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->stale_epoch_drop_count));
		values[col++] = Int32GetDatum((int32)pg_atomic_read_u32(&p->chunk_reassembly_active));
		values[col++]
			= Int64GetDatum((int64)pg_atomic_read_u64(&p->chunk_reassembly_timeout_count));
		values[col++] = Int64GetDatum((int64)pg_atomic_read_u64(&p->lamport_observe_advance_count));

		Assert(col == 23);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum)0;
}

#endif /* USE_PGRAC_CLUSTER */
