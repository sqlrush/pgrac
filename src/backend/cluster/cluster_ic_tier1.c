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
#include "cluster/cluster_ic.h"
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
static uint8 tier1_recv_buf[CLUSTER_MAX_NODES][PGRAC_IC_HEADER_BYTES];
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

static uint8 tier1_outbound_buf[CLUSTER_MAX_NODES][PGRAC_IC_HEADER_BYTES];
static int tier1_outbound_remaining[CLUSTER_MAX_NODES];


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

static bool
tier1_send_bytes(int32 target_node_id, const void *buf, size_t len)
{
	int fd;
	ssize_t sent;

	peer_fds_lazy_init();

	if (target_node_id < 0 || target_node_id >= CLUSTER_MAX_NODES) {
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_ic tier1 send: target_node_id %d out of range", target_node_id)));
		return false;
	}

	fd = tier1_peer_fds[target_node_id];
	if (fd < 0)
		return false; /* not connected */

	/*
	 * Hardening v1.0.1 F1: per-peer outbound buffer for partial writes.
	 * If a previous send for this peer left bytes pending, drain them
	 * first; only attempt the new caller-supplied buf when the buffer
	 * is empty (otherwise we'd corrupt the byte stream by interleaving
	 * frames).  Cluster-IC frames are message-aligned so partial-write
	 * recovery must NOT interleave two payloads on the same socket.
	 */
	if (tier1_outbound_remaining[target_node_id] > 0) {
		int rem = tier1_outbound_remaining[target_node_id];
		int off = (int)sizeof(tier1_outbound_buf[0]) - rem;
		ssize_t drained;

		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_SEND);
		drained = send(fd, &tier1_outbound_buf[target_node_id][off], (size_t)rem, 0);
		pgstat_report_wait_end();
		if (drained < 0) {
			int saved = errno;

			if (saved == EAGAIN || saved == EWOULDBLOCK)
				return false; /* still backpressured; caller skips */
			peer_record_error(target_node_id, saved, "08006", "send (drain): %s", strerror(saved));
			cluster_ic_tier1_close_peer(target_node_id, "send error");
			return false;
		}
		tier1_outbound_remaining[target_node_id] -= (int)drained;
		if (tier1_outbound_remaining[target_node_id] > 0)
			return false; /* still pending, defer new payload */

		if (Tier1Shmem != NULL) {
			pg_atomic_add_fetch_u64(&Tier1Shmem->peers[target_node_id].bytes_send, (uint64)drained);
			Tier1Shmem->peers[target_node_id].last_send_at = GetCurrentTimestamp();
		}
	}

	/*
	 * Nonblocking write.  Short write is now buffered (F1) instead of
	 * closing the peer: drain the remainder on next WL_SOCKET_WRITEABLE
	 * via the path above.  HEARTBEAT (24B header) fits in the buffer;
	 * spec-2.4 framing will generalize for larger payloads.
	 */
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_SEND);
	sent = send(fd, buf, len, 0);
	pgstat_report_wait_end();
	if (sent < 0) {
		int saved_errno = errno;

		if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK)
			return false; /* nonblocking; caller retries via WaitEventSet */

		/* Hard error -- mark peer down. */
		peer_record_error(target_node_id, saved_errno, "08006", "send: %s", strerror(saved_errno));
		cluster_ic_tier1_close_peer(target_node_id, "send error");
		return false;
	}

	if ((size_t)sent != len) {
		/*
		 * Hardening v1.0.1 F1: partial write -- buffer the unsent tail
		 * in tier1_outbound_buf so we can complete it on next WRITEABLE.
		 * Frame is message-aligned; we MUST complete it before the next
		 * heartbeat to avoid interleaving payloads on the wire.
		 */
		size_t tail_len = len - (size_t)sent;

		if (tail_len > sizeof(tier1_outbound_buf[0])) {
			/* Larger than the buffer (24B) -- spec-2.2 only sends
			 * 24B HEARTBEAT, so this is a programming error. */
			peer_record_error(target_node_id, 0, "08006", "partial send tail %zu > buffer %zu",
							  tail_len, sizeof(tier1_outbound_buf[0]));
			cluster_ic_tier1_close_peer(target_node_id, "send tail too large");
			return false;
		}
		memcpy(tier1_outbound_buf[target_node_id], (const char *)buf + sent, tail_len);
		tier1_outbound_remaining[target_node_id] = (int)tail_len;

		if (Tier1Shmem != NULL && sent > 0) {
			pg_atomic_add_fetch_u64(&Tier1Shmem->peers[target_node_id].bytes_send, (uint64)sent);
			Tier1Shmem->peers[target_node_id].last_send_at = GetCurrentTimestamp();
		}
		/*
		 * Return false so caller knows the send is not yet complete;
		 * buffered tail will drain on next call via the top-of-function
		 * drain path.  Heartbeat counter is NOT bumped here -- the
		 * caller (cluster_ic_tier1_send_heartbeat) bumps only on full
		 * completion.
		 */
		return false;
	}

	if (Tier1Shmem != NULL) {
		pg_atomic_add_fetch_u64(&Tier1Shmem->peers[target_node_id].bytes_send, len);
		Tier1Shmem->peers[target_node_id].last_send_at = GetCurrentTimestamp();
	}
	return true;
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

bool
cluster_ic_tier1_send_heartbeat(int32 peer_id)
{
	/*
	 * spec-2.2 §2.4 wire format -- HEARTBEAT is a 24-byte ClusterMsgHeader
	 * with msg_type = PGRAC_IC_MSG_HEARTBEAT and payload_len = 0.  The
	 * frame is built and CRC'd by cluster_msg_send (the higher-level
	 * entry point); we delegate so wire framing stays single-source.
	 *
	 * Per §3.6 boundary invariant: heartbeat carries IC transport
	 * liveness only -- it does NOT trigger fence / membership / quorum.
	 */
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES || Tier1Shmem == NULL)
		return false;
	if (Tier1Shmem->peers[peer_id].state != (int32)CLUSTER_IC_PEER_CONNECTED)
		return false;
	if (tier1_peer_fds[peer_id] < 0)
		return false;

	if (!cluster_msg_send(peer_id, PGRAC_IC_MSG_HEARTBEAT, NULL, 0))
		return false;

	pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].heartbeat_send_count, 1);
	Tier1Shmem->peers[peer_id].last_heartbeat_sent_at = GetCurrentTimestamp();
	return true;
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
		ereport(LOG, (errmsg("cluster_ic tier1 HELLO bad magic on anon slot %d", anon_slot)));
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
 * Drain pending heartbeat frames from peer_fd.  Called by LMON when
 * the per-peer fd is WL_SOCKET_READABLE in CONNECTED state.  Reads
 * non-blocking until EAGAIN, accumulating bytes into tier1_recv_buf
 * for that peer; for each complete 24-byte ClusterMsgHeader emits
 * one heartbeat_recv_count tick + last_heartbeat_recv_at update.
 *
 * Returns false on hard recv error / EOF / malformed frame -- caller
 * is expected to close_peer().  Returns true on EAGAIN (no more bytes
 * available right now) -- the loop will resume on next WL_SOCKET_READABLE.
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
		int buf_len = tier1_recv_buf_len[peer_id];
		int need = PGRAC_IC_HEADER_BYTES - buf_len;

		Assert(need > 0);

		pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_TCP_RECV);
		got = recv(peer_fd, &tier1_recv_buf[peer_id][buf_len], (size_t)need, 0);
		pgstat_report_wait_end();
		if (got < 0) {
			int saved = errno;

			if (saved == EAGAIN || saved == EWOULDBLOCK)
				return true; /* drained for now */

			peer_record_error(peer_id, saved, "08006", "heartbeat recv: %s", strerror(saved));
			return false;
		}
		if (got == 0) {
			peer_record_error(peer_id, 0, "08006", "peer closed connection (heartbeat drain)");
			return false;
		}

		buf_len += (int)got;
		tier1_recv_buf_len[peer_id] = buf_len;

		if (Tier1Shmem != NULL) {
			pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].bytes_recv, (uint64)got);
			Tier1Shmem->peers[peer_id].last_recv_at = GetCurrentTimestamp();
		}

		if (buf_len < PGRAC_IC_HEADER_BYTES)
			continue; /* partial frame, keep reading */

		/*
		 * One full header assembled.  Validate magic + msg_type; we don't
		 * verify CRC here because spec-2.2 only carries HEARTBEAT and
		 * cluster_msg_send already CRC'd it -- a corrupted frame on a
		 * loopback TCP socket is a bug, not a runtime concern (real
		 * CRC enforcement lands with general msg routing in spec-2.4).
		 */
		{
			ClusterMsgHeader hdr;

			memcpy(&hdr, tier1_recv_buf[peer_id], PGRAC_IC_HEADER_BYTES);

			if (hdr.magic != PGRAC_IC_MAGIC || hdr.msg_type != PGRAC_IC_MSG_HEARTBEAT
				|| hdr.payload_len != 0) {
				peer_record_error(peer_id, 0, "08P01",
								  "malformed heartbeat header (magic=0x%x type=%u plen=%u)",
								  hdr.magic, hdr.msg_type, hdr.payload_len);
				tier1_recv_buf_len[peer_id] = 0;
				return false;
			}

			pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].heartbeat_recv_count, 1);
			Tier1Shmem->peers[peer_id].last_heartbeat_recv_at = GetCurrentTimestamp();
			tier1_recv_buf_len[peer_id] = 0;
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
		Datum values[19];
		bool nulls[19];
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

		Assert(col == 19);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum)0;
}

#endif /* USE_PGRAC_CLUSTER */
