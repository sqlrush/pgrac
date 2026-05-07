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

typedef struct ClusterICTier1Shmem
{
	int      listener_fd;                                  /* -1 if unbound */
	int      listener_port;                                /* cached self port */
	uint32   magic;                                        /* sanity check */
	ClusterICPeerStateShmem peers[CLUSTER_MAX_NODES];
} ClusterICTier1Shmem;

#define PGRAC_IC_TIER1_SHMEM_MAGIC ((uint32)0x54494331U)   /* "TIC1" */

static ClusterICTier1Shmem *Tier1Shmem = NULL;

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
static int   tier1_recv_buf_len[CLUSTER_MAX_NODES];


/* ============================================================
 * Static helpers.
 * ============================================================ */

static inline void
peer_fds_lazy_init(void)
{
	if (!tier1_peer_fds_initialised)
	{
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
parse_host_port(const char *addr, char *out_host, size_t host_size,
				int *out_port)
{
	const char *colon;
	size_t      host_len;
	long        port;
	char       *endp;

	if (addr == NULL || addr[0] == '\0')
		return false;

	colon = strrchr(addr, ':');     /* rightmost ':' to support IPv6 [..] later */
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

	*out_port = (int) port;
	return true;
}

/*
 * Update last_error fields on a peer slot.  Caller holds whatever
 * lock its convention requires; this is just the field-write helper.
 */
static void
peer_record_error(int32 peer_id, int saved_errno, const char *errcode_str,
				  const char *fmt, ...) pg_attribute_printf(4, 5);

static void
peer_record_error(int32 peer_id, int saved_errno, const char *errcode_str,
				  const char *fmt, ...)
{
	ClusterICPeerStateShmem *p;
	va_list ap;

	if (Tier1Shmem == NULL || peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return;

	p = &Tier1Shmem->peers[peer_id];
	p->last_errno = saved_errno;
	if (errcode_str != NULL)
	{
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

	if (Tier1Shmem != NULL
		&& Tier1Shmem->peers[peer_id].interconnect_addr[0] == '\0')
	{
		strlcpy(Tier1Shmem->peers[peer_id].interconnect_addr,
				n->interconnect_addr,
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

	Tier1Shmem = (ClusterICTier1Shmem *)
		ShmemInitStruct("pgrac cluster_ic_tier1",
						tier1_shmem_size(),
						&found);

	if (!found)
	{
		int i;

		memset(Tier1Shmem, 0, tier1_shmem_size());
		Tier1Shmem->magic = PGRAC_IC_TIER1_SHMEM_MAGIC;
		Tier1Shmem->listener_fd = -1;
		Tier1Shmem->listener_port = -1;

		for (i = 0; i < CLUSTER_MAX_NODES; i++)
		{
			Tier1Shmem->peers[i].node_id = -1;
			Tier1Shmem->peers[i].state = (int32) CLUSTER_IC_PEER_DOWN;
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
	int     fd;
	ssize_t sent;

	peer_fds_lazy_init();

	if (target_node_id < 0 || target_node_id >= CLUSTER_MAX_NODES)
	{
		ereport(WARNING,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_ic tier1 send: target_node_id %d out of range",
						target_node_id)));
		return false;
	}

	fd = tier1_peer_fds[target_node_id];
	if (fd < 0)
		return false;       /* not connected */

	/*
	 * Nonblocking write.  In Step 6 we treat short writes as failure;
	 * Step 7 LMON main loop will retry on WL_SOCKET_WRITEABLE.  Step 6
	 * standalone send_bytes is reachable only from cluster_unit smoke
	 * (where peer_fds[] are still -1) and never executes the write.
	 */
	sent = send(fd, buf, len, 0);
	if (sent < 0)
	{
		int saved_errno = errno;

		if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK)
			return false;       /* nonblocking; caller retries via WaitEventSet */

		/* Hard error -- mark peer down. */
		peer_record_error(target_node_id, saved_errno, "08006",
						  "send: %s", strerror(saved_errno));
		cluster_ic_tier1_close_peer(target_node_id, "send error");
		return false;
	}

	if ((size_t) sent != len)
	{
		/* Partial write -- caller (LMON) is responsible for retry. */
		return false;
	}

	if (Tier1Shmem != NULL)
	{
		pg_atomic_add_fetch_u64(&Tier1Shmem->peers[target_node_id].bytes_send, len);
		Tier1Shmem->peers[target_node_id].last_send_at = GetCurrentTimestamp();
	}
	return true;
}

static bool
tier1_peek_sender(int32 *out_sender_node_id)
{
	fd_set         rfds;
	struct timeval tv = { 0, 0 };
	int            max_fd = -1;
	int            i;
	int            ready;

	peer_fds_lazy_init();

	FD_ZERO(&rfds);
	for (i = 0; i < CLUSTER_MAX_NODES; i++)
	{
		int fd = tier1_peer_fds[i];

		if (fd >= 0)
		{
			FD_SET(fd, &rfds);
			if (fd > max_fd)
				max_fd = fd;
		}
	}

	if (max_fd < 0)
		return false;       /* no connections -- never reached in Step 7 normal flow */

	ready = select(max_fd + 1, &rfds, NULL, NULL, &tv);
	if (ready <= 0)
		return false;       /* nothing ready / interrupted */

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
	{
		int fd = tier1_peer_fds[i];

		if (fd >= 0 && FD_ISSET(fd, &rfds))
		{
			if (out_sender_node_id != NULL)
				*out_sender_node_id = (int32) i;
			return true;
		}
	}
	return false;
}

static bool
tier1_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize,
				 size_t *out_received_len)
{
	int32   sender = -1;
	int     fd;
	ssize_t got;

	peer_fds_lazy_init();

	if (out_received_len != NULL)
		*out_received_len = 0;
	if (out_sender_node_id != NULL)
		*out_sender_node_id = -1;

	if (!tier1_peek_sender(&sender))
		return true;            /* strict: no data => true with received=0 */

	Assert(sender >= 0 && sender < CLUSTER_MAX_NODES);
	fd = tier1_peer_fds[sender];
	if (fd < 0)
		return true;            /* race: closed between peek and now */

	got = recv(fd, buf, bufsize, 0);
	if (got < 0)
	{
		int saved_errno = errno;

		if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK)
			return true;       /* race after peek; treat as no data */

		/* Hard error per spec-2.2 D2 strict bool semantics. */
		peer_record_error(sender, saved_errno, "08006",
						  "recv: %s", strerror(saved_errno));
		cluster_ic_tier1_close_peer(sender, "recv error");
		return false;
	}
	if (got == 0)
	{
		/* Peer closed connection cleanly -- treat as hard error so
		 * recv_exact loop propagates and LMON drops the peer state. */
		peer_record_error(sender, 0, "08006", "peer closed connection");
		cluster_ic_tier1_close_peer(sender, "peer EOF");
		return false;
	}

	if (out_sender_node_id != NULL)
		*out_sender_node_id = sender;
	if (out_received_len != NULL)
		*out_received_len = (size_t) got;

	if (Tier1Shmem != NULL)
	{
		pg_atomic_add_fetch_u64(&Tier1Shmem->peers[sender].bytes_recv, (uint64) got);
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
	ereport(LOG,
			(errmsg("cluster_ic tier1 vtable bound; listener will bind in LMON main loop")));
}

static void
tier1_tier_shutdown(void)
{
	int i;

	peer_fds_lazy_init();

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
	{
		if (tier1_peer_fds[i] >= 0)
		{
			(void) close(tier1_peer_fds[i]);
			tier1_peer_fds[i] = -1;
		}
	}

	if (Tier1Shmem != NULL && Tier1Shmem->listener_fd >= 0)
	{
		(void) close(Tier1Shmem->listener_fd);
		Tier1Shmem->listener_fd = -1;
	}
}

const ClusterICOps ClusterICOps_Tier1 = {
	.send_bytes  = tier1_send_bytes,
	.recv_bytes  = tier1_recv_bytes,
	.peek_sender = tier1_peek_sender,
	.tier_init   = tier1_tier_init,
	.tier_shutdown = tier1_tier_shutdown,
	.tier_name   = "tier1",
};


/* ============================================================
 * LMON-internal API.
 * ============================================================ */

int
cluster_ic_tier1_listener_bind(void)
{
	const ClusterNodeInfo *self;
	char    self_host[CLUSTER_IC_TIER1_ADDR_LEN];
	int     self_port;
	int     fd;
	int     yes = 1;
	struct sockaddr_in sa;

	peer_fds_lazy_init();

	if (Tier1Shmem == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cluster_ic tier1 shmem not initialised before listener_bind")));

	if (Tier1Shmem->listener_fd >= 0)
		return Tier1Shmem->listener_fd;     /* already bound (re-entry safe) */

	self = cluster_conf_lookup_node(cluster_node_id);
	if (self == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("cluster_ic tier1: cluster.node_id=%d not in pgrac.conf",
						cluster_node_id),
				 errhint("Add a [node.%d] section to pgrac.conf with "
						 "interconnect_addr.", cluster_node_id)));

	if (!parse_host_port(self->interconnect_addr, self_host,
						 sizeof(self_host), &self_port))
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("cluster_ic tier1: cannot parse interconnect_addr \"%s\"",
						self->interconnect_addr)));

	fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd < 0)
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1: socket() failed: %m")));

	(void) setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
	(void) setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

	if (set_socket_nonblocking(fd) < 0)
	{
		int saved = errno;

		(void) close(fd);
		errno = saved;
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1: fcntl O_NONBLOCK failed: %m")));
	}

	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_port = htons((unsigned short) self_port);
	if (inet_pton(AF_INET, self_host, &sa.sin_addr) != 1)
	{
		(void) close(fd);
		ereport(FATAL,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("cluster_ic tier1: inet_pton failed for \"%s\"",
						self_host),
				 errhint("Stage 2 spec-2.2 supports IPv4 dotted-quad only; "
						 "IPv6 [::] forms land in a future spec.")));
	}

	if (bind(fd, (struct sockaddr *) &sa, sizeof(sa)) < 0)
	{
		int saved = errno;

		(void) close(fd);
		errno = saved;
		/*
		 * spec-2.2 §3.10: listener bind failure is the ONLY transport-
		 * setup failure that escalates to FATAL; LMON cannot do its job
		 * without a listener.
		 */
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1: bind on %s:%d failed: %m",
						self_host, self_port)));
	}

	if (listen(fd, /* backlog */ 16) < 0)
	{
		int saved = errno;

		(void) close(fd);
		errno = saved;
		ereport(FATAL,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1: listen on %s:%d failed: %m",
						self_host, self_port)));
	}

	Tier1Shmem->listener_fd = fd;
	Tier1Shmem->listener_port = self_port;

	ereport(LOG,
			(errmsg("cluster_ic tier1 listener bound on %s:%d (fd=%d)",
					self_host, self_port, fd)));
	return fd;
}

bool
cluster_ic_tier1_accept_one(int *out_peer_fd, int32 *out_peer_id)
{
	int                 listener_fd;
	int                 cfd;
	struct sockaddr_in  ca;
	socklen_t           clen = sizeof(ca);

	peer_fds_lazy_init();

	if (out_peer_fd != NULL) *out_peer_fd = -1;
	if (out_peer_id != NULL) *out_peer_id = -1;

	listener_fd = cluster_ic_tier1_get_listener_fd();
	if (listener_fd < 0)
		return false;

	cfd = accept(listener_fd, (struct sockaddr *) &ca, &clen);
	if (cfd < 0)
	{
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			return false;       /* no pending connection */
		ereport(WARNING,
				(errcode_for_socket_access(),
				 errmsg("cluster_ic tier1 accept(): %m")));
		return false;
	}

	if (set_socket_nonblocking(cfd) < 0)
	{
		(void) close(cfd);
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
	if (out_peer_fd != NULL) *out_peer_fd = cfd;
	if (out_peer_id != NULL) *out_peer_id = -1;
	return true;
}

bool
cluster_ic_tier1_connect_one(int32 peer_id, int *out_peer_fd)
{
	const char *addr;
	char        host[CLUSTER_IC_TIER1_ADDR_LEN];
	int         port;
	int         fd;
	int         yes = 1;
	struct sockaddr_in sa;
	int         rc;

	peer_fds_lazy_init();

	if (out_peer_fd != NULL) *out_peer_fd = -1;

	addr = peer_addr(peer_id);
	if (addr == NULL)
	{
		peer_record_error(peer_id, 0, "08001",
						  "peer not declared in pgrac.conf");
		return false;
	}
	if (!parse_host_port(addr, host, sizeof(host), &port))
	{
		peer_record_error(peer_id, 0, "08001",
						  "bad interconnect_addr \"%s\"", addr);
		return false;
	}

	fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd < 0)
	{
		int saved = errno;

		peer_record_error(peer_id, saved, "08001",
						  "socket: %s", strerror(saved));
		return false;
	}

	(void) setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
	if (set_socket_nonblocking(fd) < 0)
	{
		int saved = errno;

		(void) close(fd);
		peer_record_error(peer_id, saved, "08001",
						  "fcntl O_NONBLOCK: %s", strerror(saved));
		return false;
	}

	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_port = htons((unsigned short) port);
	if (inet_pton(AF_INET, host, &sa.sin_addr) != 1)
	{
		(void) close(fd);
		peer_record_error(peer_id, 0, "08001",
						  "inet_pton failed for \"%s\"", host);
		return false;
	}

	rc = connect(fd, (struct sockaddr *) &sa, sizeof(sa));
	if (rc < 0 && errno != EINPROGRESS)
	{
		int saved = errno;

		(void) close(fd);
		peer_record_error(peer_id, saved, "08001",
						  "connect %s:%d: %s", host, port, strerror(saved));
		Tier1Shmem->peers[peer_id].connect_error_count++;
		return false;
	}

	tier1_peer_fds[peer_id] = fd;
	Tier1Shmem->peers[peer_id].state = (int32) CLUSTER_IC_PEER_CONNECTING;
	if (out_peer_fd != NULL) *out_peer_fd = fd;
	return true;
}

bool
cluster_ic_tier1_finish_connect(int32 peer_id, int peer_fd)
{
	int       so_error = 0;
	socklen_t so_error_len = sizeof(so_error);
	uint8     hello_buf[PGRAC_IC_HELLO_BYTES];
	ssize_t   sent;
	const char *self_cluster_name;

	if (getsockopt(peer_fd, SOL_SOCKET, SO_ERROR, &so_error, &so_error_len) < 0
		|| so_error != 0)
	{
		int saved = (so_error != 0) ? so_error : errno;

		peer_record_error(peer_id, saved, "08001",
						  "connect SO_ERROR: %s", strerror(saved));
		cluster_ic_tier1_close_peer(peer_id, "connect failed");
		return false;
	}

	/* Send HELLO (active edge speaks first per spec-2.2 §2.4). */
	self_cluster_name = (ClusterConfShmem != NULL)
		? ClusterConfShmem->cluster_name
		: "";
	cluster_ic_build_hello(hello_buf,
						   PGRAC_IC_HELLO_VERSION_V1,
						   PGRAC_IC_ENVELOPE_VERSION_V1,
						   cluster_node_id,
						   self_cluster_name);

	sent = send(peer_fd, hello_buf, PGRAC_IC_HELLO_BYTES, 0);
	if (sent != PGRAC_IC_HELLO_BYTES)
	{
		int saved = (sent < 0) ? errno : 0;

		peer_record_error(peer_id, saved, "08006",
						  "HELLO send short or failed (%zd of %d): %s",
						  sent, PGRAC_IC_HELLO_BYTES,
						  saved ? strerror(saved) : "short write");
		cluster_ic_tier1_close_peer(peer_id, "HELLO send failed");
		return false;
	}

	/*
	 * spec-2.2 §2.4 -- protocol is asymmetric: active sender considers
	 * itself CONNECTED after a successful HELLO send.  Passive verifier
	 * reads the HELLO and either CONNECTEDs or rejects + closes (active
	 * detects rejection on next heartbeat send/recv).  No HELLO_ACK.
	 */
	if (Tier1Shmem != NULL)
	{
		Tier1Shmem->peers[peer_id].state = (int32) CLUSTER_IC_PEER_CONNECTED;
		Tier1Shmem->peers[peer_id].last_connect_at = GetCurrentTimestamp();
	}
	ereport(LOG,
			(errmsg("cluster_ic tier1 peer %d HELLO sent, state CONNECTED (active)",
					peer_id)));
	return true;
}

bool
cluster_ic_tier1_recv_and_verify_hello(int32 peer_id, int peer_fd)
{
	uint8           hello_buf[PGRAC_IC_HELLO_BYTES];
	ssize_t         got;
	ClusterICHelloMsg msg;
	const char *self_cluster_name;
	const ClusterNodeInfo *peer_info;

	got = recv(peer_fd, hello_buf, PGRAC_IC_HELLO_BYTES, MSG_WAITALL);
	if (got != PGRAC_IC_HELLO_BYTES)
	{
		int saved = (got < 0) ? errno : 0;

		peer_record_error(peer_id, saved, "08P01",
						  "HELLO recv short or failed (%zd of %d): %s",
						  got, PGRAC_IC_HELLO_BYTES,
						  saved ? strerror(saved) : "short read");
		cluster_ic_tier1_close_peer(peer_id, "HELLO recv failed");
		return false;
	}

	if (!cluster_ic_parse_hello(hello_buf, &msg))
	{
		peer_record_error(peer_id, 0, "08P01", "HELLO bad magic");
		cluster_ic_tier1_close_peer(peer_id, "HELLO bad magic");
		Tier1Shmem->peers[peer_id].state = (int32) CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	if (msg.hello_version != PGRAC_IC_HELLO_VERSION_V1
		|| msg.envelope_version != PGRAC_IC_ENVELOPE_VERSION_V1)
	{
		peer_record_error(peer_id, 0, "08P01",
						  "HELLO version mismatch (hello=%u env=%u)",
						  msg.hello_version, msg.envelope_version);
		cluster_ic_tier1_close_peer(peer_id, "HELLO version mismatch");
		Tier1Shmem->peers[peer_id].state = (int32) CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	self_cluster_name = (ClusterConfShmem != NULL)
		? ClusterConfShmem->cluster_name
		: "";
	if (ClusterConfShmem != NULL
		&& strcmp(msg.cluster_name, self_cluster_name) != 0)
	{
		peer_record_error(peer_id, 0, "08P01",
						  "HELLO cluster_name mismatch (peer=\"%s\" mine=\"%s\")",
						  msg.cluster_name, self_cluster_name);
		cluster_ic_tier1_close_peer(peer_id, "HELLO cluster_name mismatch");
		Tier1Shmem->peers[peer_id].state = (int32) CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	peer_info = cluster_conf_lookup_node(msg.source_node_id);
	if (peer_info == NULL)
	{
		peer_record_error(peer_id, 0, "08P01",
						  "HELLO unknown source_node_id %d", msg.source_node_id);
		cluster_ic_tier1_close_peer(peer_id, "HELLO unknown peer");
		Tier1Shmem->peers[peer_id].state = (int32) CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	if (peer_id >= 0 && peer_id != msg.source_node_id)
	{
		peer_record_error(peer_id, 0, "08P01",
						  "HELLO source_node_id %d != expected %d",
						  msg.source_node_id, peer_id);
		cluster_ic_tier1_close_peer(peer_id, "HELLO peer id mismatch");
		Tier1Shmem->peers[peer_id].state = (int32) CLUSTER_IC_PEER_REJECTED;
		return false;
	}

	/* On accept side peer_id was -1 until now; bind fd to learned peer. */
	if (peer_id < 0)
	{
		peer_id = msg.source_node_id;
		tier1_peer_fds[peer_id] = peer_fd;
	}

	Tier1Shmem->peers[peer_id].state = (int32) CLUSTER_IC_PEER_CONNECTED;
	Tier1Shmem->peers[peer_id].last_connect_at = GetCurrentTimestamp();
	(void) peer_addr(peer_id);     /* cache addr in shmem for view */

	ereport(LOG,
			(errmsg("cluster_ic tier1 peer %d HELLO verified, state CONNECTED",
					peer_id)));
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
	if (Tier1Shmem->peers[peer_id].state != (int32) CLUSTER_IC_PEER_CONNECTED)
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

	for (;;)
	{
		ssize_t got;
		int     buf_len = tier1_recv_buf_len[peer_id];
		int     need    = PGRAC_IC_HEADER_BYTES - buf_len;

		Assert(need > 0);

		got = recv(peer_fd, &tier1_recv_buf[peer_id][buf_len],
				   (size_t) need, 0);
		if (got < 0)
		{
			int saved = errno;

			if (saved == EAGAIN || saved == EWOULDBLOCK)
				return true;       /* drained for now */

			peer_record_error(peer_id, saved, "08006",
							  "heartbeat recv: %s", strerror(saved));
			return false;
		}
		if (got == 0)
		{
			peer_record_error(peer_id, 0, "08006",
							  "peer closed connection (heartbeat drain)");
			return false;
		}

		buf_len += (int) got;
		tier1_recv_buf_len[peer_id] = buf_len;

		if (Tier1Shmem != NULL)
		{
			pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].bytes_recv,
									(uint64) got);
			Tier1Shmem->peers[peer_id].last_recv_at = GetCurrentTimestamp();
		}

		if (buf_len < PGRAC_IC_HEADER_BYTES)
			continue;              /* partial frame, keep reading */

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

			if (hdr.magic != PGRAC_IC_MAGIC
				|| hdr.msg_type != PGRAC_IC_MSG_HEARTBEAT
				|| hdr.payload_len != 0)
			{
				peer_record_error(peer_id, 0, "08P01",
								  "malformed heartbeat header (magic=0x%x type=%u plen=%u)",
								  hdr.magic, hdr.msg_type, hdr.payload_len);
				tier1_recv_buf_len[peer_id] = 0;
				return false;
			}

			pg_atomic_add_fetch_u64(&Tier1Shmem->peers[peer_id].heartbeat_recv_count,
									1);
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

	if (tier1_peer_fds[peer_id] >= 0)
	{
		(void) close(tier1_peer_fds[peer_id]);
		tier1_peer_fds[peer_id] = -1;
	}

	if (Tier1Shmem != NULL)
	{
		/* Don't overwrite REJECTED state -- that's a stickier verdict. */
		if (Tier1Shmem->peers[peer_id].state != (int32) CLUSTER_IC_PEER_REJECTED)
			Tier1Shmem->peers[peer_id].state = (int32) CLUSTER_IC_PEER_DOWN;
		Tier1Shmem->peers[peer_id].reconnect_count++;
	}

	if (reason != NULL)
		ereport(LOG,
				(errmsg("cluster_ic tier1 peer %d closed: %s",
						peer_id, reason)));
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
	if (Tier1Shmem == NULL)
		return -1;
	return Tier1Shmem->listener_fd;
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
	switch ((ClusterICPeerState) s)
	{
		case CLUSTER_IC_PEER_DOWN:       return "down";
		case CLUSTER_IC_PEER_CONNECTING: return "connecting";
		case CLUSTER_IC_PEER_CONNECTED:  return "connected";
		case CLUSTER_IC_PEER_REJECTED:   return "rejected";
	}
	return "unknown";
}

Datum
cluster_get_ic_peers(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	int            i;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	if (Tier1Shmem == NULL)
		PG_RETURN_VOID();

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
	{
		ClusterICPeerStateShmem *p = &Tier1Shmem->peers[i];
		Datum   values[19];
		bool    nulls[19];
		int     col = 0;

		if (cluster_conf_lookup_node(i) == NULL)
			continue;       /* peer not declared in pgrac.conf */

		memset(nulls, false, sizeof(nulls));

		values[col++] = Int32GetDatum(i);
		values[col++] = CStringGetTextDatum(peer_state_to_string(p->state));
		values[col++] = CStringGetTextDatum(p->interconnect_addr[0]
											? p->interconnect_addr : "");
#define ADD_TS(field) \
	do { \
		if (p->field == 0) \
			nulls[col] = true; \
		else \
			values[col] = TimestampTzGetDatum(p->field); \
		col++; \
	} while (0)
		ADD_TS(last_connect_at);
		ADD_TS(last_send_at);
		ADD_TS(last_recv_at);
		ADD_TS(last_heartbeat_sent_at);
		ADD_TS(last_heartbeat_recv_at);
#undef ADD_TS
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&p->heartbeat_send_count));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&p->heartbeat_recv_count));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&p->msg_send_count));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&p->msg_recv_count));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&p->bytes_send));
		values[col++] = Int64GetDatum((int64) pg_atomic_read_u64(&p->bytes_recv));
		values[col++] = Int32GetDatum((int32) p->reconnect_count);
		values[col++] = Int32GetDatum((int32) p->connect_error_count);
		values[col++] = Int32GetDatum(p->last_errno);
		values[col++] = CStringGetTextDatum(p->last_error_code[0]
											? p->last_error_code : "");
		values[col++] = CStringGetTextDatum(p->last_error[0] ? p->last_error : "");

		Assert(col == 19);
		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum) 0;
}

#endif /* USE_PGRAC_CLUSTER */
