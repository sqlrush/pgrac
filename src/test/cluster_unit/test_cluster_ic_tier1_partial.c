/*-------------------------------------------------------------------------
 *
 * test_cluster_ic_tier1_partial.c
 *	  Deterministic partial-IO invariants for the tier1 outbound tail
 *	  state machine (GCS-race round-4c tier1-partial-IO F1/F2/F4 — the
 *	  ~56s retransmit-budget-wall root causes).
 *
 *	  Links cluster_ic_tier1.o standalone and drives the PRODUCTION
 *	  send/drain paths over a real localhost TCP pair:
 *
 *	    T-1  connect_one registers the peer fd (production registration
 *	         path — no test seam into tier1_peer_fds).
 *	    T-2  frame A (8 KB) against a full socket: initial-EAGAIN queues
 *	         the full frame (WOULD_BLOCK + pending_outbound).
 *	    T-3  drain delivers frame A intact ('A' bytes only).
 *	    T-4  frame B (4 KB) against a full socket AFTER the dyn buffer
 *	         grew to 8 KB: the resume cursor must be measured against the
 *	         QUEUED TAIL LENGTH, not the grow-only allocation capacity.
 *	         Pre-fix code computed off = capacity - remaining and put
 *	         stale frame-A bytes on the wire (10/10 reproducible).
 *	    T-5  drain delivers frame B intact ('B' bytes only) — the F1
 *	         regression proof.
 *	    T-6  pending_outbound drops to false + writable_drain counter
 *	         moved (the F2 drain-entry observability contract).
 *	    T-7  a queued tail does NOT survive close_peer (F4): a byte-
 *	         stream continuation must never poison a reconnected socket.
 *	    T-8  drain entry on a dead peer (fd gone) fail-closes HARD_ERROR.
 *
 *	  The WL_SOCKET_WRITEABLE *wiring* of the DATA-plane tick
 *	  (cluster_lms_data_plane.c) cannot run standalone — WaitEventSet /
 *	  latch machinery needs a live backend — so the tick-level arm is
 *	  covered by the loaded S3 regression via the
 *	  ic.tier1_writable_drain_data dump row instead (commit message
 *	  records this test boundary).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_ic_tier1_partial.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_chunk.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_shmem.h"
#include "funcapi.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf and we don't rely on pg_printf in this binary. */
#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();

/* ============================================================
 * PG-runtime stubs.
 * ============================================================ */

int cluster_node_id = 0;
static uint32 ut_wait_event_info_storage = 0;
uint32 *my_wait_event_info = &ut_wait_event_info_storage;

/* tier1 TCP keepalive GUCs (apply_tcp_keepalive). */
int cluster_interconnect_tcp_keepidle_sec = 5;
int cluster_interconnect_tcp_keepintvl_sec = 1;
int cluster_interconnect_tcp_keepcnt = 3;

/* conf shmem pointer consumed for cluster_name defaults. */
ClusterConf *ClusterConfShmem = NULL;

/* HELLO builder stubs: finish_connect seeds a 64-byte HELLO through these
 * (cluster_ic.c is not linked);  the bytes land ahead of the junk fill and
 * are ignored by the tail-anchored assertions below.  Flipping the peer to
 * CONNECTED still runs the PRODUCTION continue_hello_send path. */
void
cluster_ic_build_hello(uint8 out_buf[PGRAC_IC_HELLO_BYTES], uint16 hello_version,
					   uint16 envelope_version, int32 source_node_id, const char *cluster_name,
					   ClusterICPlane plane, uint64 conn_epoch)
{
	(void)hello_version;
	(void)envelope_version;
	(void)source_node_id;
	(void)cluster_name;
	(void)plane;
	(void)conn_epoch;
	memset(out_buf, 'H', PGRAC_IC_HELLO_BYTES);
}

void
cluster_ic_hello_set_worker_fields(uint8 out_buf[PGRAC_IC_HELLO_BYTES], uint8 worker_id,
								   uint8 n_workers)
{
	(void)out_buf;
	(void)worker_id;
	(void)n_workers;
}

#define UT_PEER_ID 3
#define UT_PEER2_ID 5
#define UT_LISTEN_BACKLOG 4

static ClusterNodeInfo ut_peer_info;
static bool ut_peer_declared = false;
static ClusterNodeInfo ut_peer2_info;
static bool ut_peer2_declared = false;

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	if (ut_peer_declared && node_id == UT_PEER_ID)
		return &ut_peer_info;
	if (ut_peer2_declared && node_id == UT_PEER2_ID)
		return &ut_peer2_info;
	return NULL;
}

uint64
cluster_epoch_get_current(void)
{
	return 1;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

/* Captured shmem region: init against a malloc'd block. */
static const ClusterShmemRegion *ut_captured_region = NULL;

void
cluster_shmem_register_region(const ClusterShmemRegion *region)
{
	ut_captured_region = region;
}

void *
ShmemInitStruct(const char *name, Size size, bool *foundPtr)
{
	void *p = malloc(size);

	(void)name;
	UT_ASSERT(p != NULL);
	memset(p, 0, size);
	*foundPtr = false;
	return p;
}

/* Memory context stubs: tier1 lazy-pallocs its dyn buffers. */
MemoryContext TopMemoryContext = (MemoryContext)&TopMemoryContext;
MemoryContext CurrentMemoryContext = (MemoryContext)&TopMemoryContext;

void *
palloc(Size size)
{
	void *p = malloc(size);

	UT_ASSERT(p != NULL);
	return p;
}

void
pfree(void *pointer)
{
	free(pointer);
}

/* ereport plumbing: swallow everything (unit binary has no elog.c). */
bool
errstart(int elevel, const char *domain)
{
	(void)elevel;
	(void)domain;
	return false; /* suppress body evaluation */
}

void
errfinish(const char *filename, int lineno, const char *funcname)
{
	(void)filename;
	(void)lineno;
	(void)funcname;
}

int
errcode(int sqlerrcode)
{
	(void)sqlerrcode;
	return 0;
}

int
errcode_for_socket_access(void)
{
	return 0;
}

int
errmsg(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

int
errmsg_internal(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

int
errdetail(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

int
errhint(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}

bool
errstart_cold(int elevel, const char *domain)
{
	(void)elevel;
	(void)domain;
	return false;
}

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

int MyProcPid = 12345;

/* SRF plumbing referenced by the tier1 peers view — never called here. */
void
InitMaterializedSRF(FunctionCallInfo fcinfo, bits32 flags)
{
	(void)fcinfo;
	(void)flags;
	abort();
}

void
tuplestore_putvalues(Tuplestorestate *state, TupleDesc tdesc, Datum *values, bool *isnull)
{
	(void)state;
	(void)tdesc;
	(void)values;
	(void)isnull;
	abort();
}

text *
cstring_to_text(const char *s)
{
	(void)s;
	abort();
	return NULL;
}

/* Router / envelope / chunk / smart-fusion deps of the recv paths —
 * the test never receives an envelope, so these are vacuous. */
bool cluster_ic_suppress_caps_reply = false;

bool
cluster_ic_parse_hello(const uint8 in_buf[PGRAC_IC_HELLO_BYTES], ClusterICHelloMsg *out_msg)
{
	(void)in_buf;
	(void)out_msg;
	return false;
}

void
cluster_ic_register_msg_type(const ClusterICMsgTypeInfo *info)
{
	(void)info;
}

ClusterICSendResult
cluster_ic_send_envelope(uint8 msg_type, int32 dest_node_id, const void *payload,
						 uint32 payload_len)
{
	(void)msg_type;
	(void)dest_node_id;
	(void)payload;
	(void)payload_len;
	return CLUSTER_IC_SEND_DONE;
}

bool
cluster_ic_dispatch_envelope(const ClusterICEnvelope *env, const void *payload, int32 peer_id)
{
	(void)env;
	(void)payload;
	(void)peer_id;
	return true;
}

ClusterICEnvelopeVerifyResult
cluster_ic_envelope_accept_and_observe(const ClusterICEnvelope *env, const void *payload,
									   uint32 payload_len, uint32 self_node_id, int32 peer_id)
{
	(void)env;
	(void)payload;
	(void)payload_len;
	(void)self_node_id;
	(void)peer_id;
	return CLUSTER_IC_ENVELOPE_OK;
}

void
cluster_ic_chunk_reset_peer(int32 peer_id)
{
	(void)peer_id;
}

void
cluster_sf_note_peer_hello_capabilities_gen(int32 peer_id, uint32 capabilities, uint32 generation)
{
	(void)peer_id;
	(void)capabilities;
	(void)generation;
}

void
cluster_sf_note_peer_disconnected_gen(int32 peer_id, uint32 generation)
{
	(void)peer_id;
	(void)generation;
}

void
cluster_sf_note_peer_disconnected(int32 peer_id)
{
	(void)peer_id;
}

void
cluster_sf_note_caps_reply_rejected(void)
{}

/* ============================================================
 * Test harness helpers.
 * ============================================================ */

/* Nonblocking localhost listener on an ephemeral port. */
static int
ut_open_listener(int *out_port)
{
	int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	struct sockaddr_in sa;
	socklen_t salen = sizeof(sa);
	int yes = 1;

	UT_ASSERT(fd >= 0);
	(void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
	memset(&sa, 0, sizeof(sa));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
	sa.sin_port = 0;
	UT_ASSERT(bind(fd, (struct sockaddr *)&sa, sizeof(sa)) == 0);
	UT_ASSERT(listen(fd, UT_LISTEN_BACKLOG) == 0);
	UT_ASSERT(getsockname(fd, (struct sockaddr *)&sa, &salen) == 0);
	*out_port = (int)ntohs(sa.sin_port);
	return fd;
}

/* Fill the peer's send path with junk until EAGAIN (both socket buffers
 * full).  Returns the number of junk bytes written. */
static long
ut_fill_until_eagain(int fd)
{
	char junk[4096];
	long total = 0;

	memset(junk, 'J', sizeof(junk));
	for (;;) {
		ssize_t n = send(fd, junk, sizeof(junk), 0);

		if (n < 0) {
			UT_ASSERT(errno == EAGAIN || errno == EWOULDBLOCK);
			break;
		}
		total += (long)n;
		UT_ASSERT(total < 64L * 1024 * 1024); /* runaway guard */
	}
	return total;
}

/* Pump the production drain entry until DONE, receiving concurrently so
 * the kernel buffers empty.  Collects the stream into acc and returns once
 * `expected` bytes arrived AND nothing is pending — a fixed byte target,
 * because loopback delivery of the final drained tail is asynchronous (a
 * bare "queue empty + one nonblocking sweep" races under load). */
static long
ut_drain_and_collect(int rx_fd, char *acc, long acc_cap, long expected)
{
	long collected = 0;
	int idle_spins = 0;

	UT_ASSERT(expected <= acc_cap);
	for (;;) {
		ssize_t n = recv(rx_fd, acc + collected, (size_t)(acc_cap - collected), MSG_DONTWAIT);

		if (n > 0) {
			collected += (long)n;
			idle_spins = 0;
		}

		if (collected >= expected && !cluster_ic_tier1_pending_outbound(UT_PEER_ID))
			break;

		if (cluster_ic_tier1_pending_outbound(UT_PEER_ID)) {
			ClusterICSendResult rc = cluster_ic_tier1_drain_outbound(UT_PEER_ID);

			UT_ASSERT(rc == CLUSTER_IC_SEND_DONE || rc == CLUSTER_IC_SEND_WOULD_BLOCK);
		}
		if (n <= 0) {
			usleep(1000);
			UT_ASSERT(++idle_spins < 10000); /* 10s bound */
		}
	}
	return collected;
}

/*
 * Drain until nothing is pending, sweeping the wire concurrently, and keep
 * sweeping through a short quiet window after the queue empties.  Unlike
 * ut_drain_and_collect this does NOT take an expected byte count — the
 * multi-frame legs assert on what actually arrived, so a LOST frame shows
 * up as missing bytes in the accumulator instead of a harness timeout.
 */
static long
ut_drain_all_and_sweep(int32 peer_id, int rx_fd, char *acc, long acc_cap)
{
	long collected = 0;
	int idle_spins = 0;

	for (;;) {
		ssize_t n = recv(rx_fd, acc + collected, (size_t)(acc_cap - collected), MSG_DONTWAIT);

		if (n > 0) {
			collected += (long)n;
			idle_spins = 0;
		}

		if (cluster_ic_tier1_pending_outbound(peer_id)) {
			ClusterICSendResult rc = cluster_ic_tier1_drain_outbound(peer_id);

			UT_ASSERT(rc == CLUSTER_IC_SEND_DONE || rc == CLUSTER_IC_SEND_WOULD_BLOCK);
		} else if (n <= 0 && ++idle_spins > 200) {
			break; /* ~200ms of quiet with nothing pending: stream settled */
		}
		if (n <= 0)
			usleep(1000);
	}
	return collected;
}

/* ============================================================
 * Tests.
 * ============================================================ */

static int ut_tx_fd = -1;			  /* tier1's registered peer fd (we hold a copy) */
static int ut_rx_fd = -1;			  /* our end of the wire */
static int ut_tx2_fd = -1;			  /* second peer: tier1's fd */
static int ut_rx2_fd = -1;			  /* second peer: our end */
static char ut_acc[64 * 1024 * 1024]; /* shared stream accumulator */
static long ut_junk_a = 0;			  /* junk bytes written ahead of frame A / frame B */
static long ut_junk_b = 0;

UT_TEST(test_connect_registers_peer_fd)
{
	int listener;
	int port;
	struct sockaddr_in sa;
	socklen_t salen = sizeof(sa);
	fd_set wfds;
	struct timeval tv;

	/* Bring up the fake tier1 shmem through the production region hooks. */
	cluster_ic_tier1_shmem_register();
	UT_ASSERT(ut_captured_region != NULL);
	ut_captured_region->init_fn();

	listener = ut_open_listener(&port);
	snprintf(ut_peer_info.interconnect_addr, sizeof(ut_peer_info.interconnect_addr), "127.0.0.1:%d",
			 port);
	ut_peer_info.node_id = UT_PEER_ID;
	ut_peer_declared = true;

	UT_ASSERT(cluster_ic_tier1_connect_one(UT_PEER_ID, &ut_tx_fd));
	UT_ASSERT(ut_tx_fd >= 0);

	/* Accept our end + wait for the nonblocking connect to complete. */
	ut_rx_fd = accept(listener, (struct sockaddr *)&sa, &salen);
	UT_ASSERT(ut_rx_fd >= 0);
	FD_ZERO(&wfds);
	FD_SET(ut_tx_fd, &wfds);
	tv.tv_sec = 5;
	tv.tv_usec = 0;
	UT_ASSERT(select(ut_tx_fd + 1, NULL, &wfds, NULL, &tv) == 1);

	/* Production CONNECTED flip: finish_connect seeds + pushes the 64-byte
	 * HELLO (stubbed builder above), and continue_hello_send flips the peer
	 * state to CONNECTED once the last HELLO byte is on the wire. */
	UT_ASSERT(cluster_ic_tier1_finish_connect(UT_PEER_ID, ut_tx_fd));
	UT_ASSERT(cluster_ic_tier1_hello_send_remaining(UT_PEER_ID) == 0);
	(void)close(listener);
}

UT_TEST(test_initial_eagain_queues_full_frame)
{
	static char frame_a[8192];
	ClusterICSendResult rc;

	memset(frame_a, 'A', sizeof(frame_a));
	ut_junk_a = ut_fill_until_eagain(ut_tx_fd);

	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_a, sizeof(frame_a));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);
	UT_ASSERT(cluster_ic_tier1_pending_outbound(UT_PEER_ID));
}

UT_TEST(test_drain_delivers_frame_a_intact)
{
	long got;
	long i;
	long a_seen = 0;

	got = ut_drain_and_collect(ut_rx_fd, ut_acc, (long)sizeof(ut_acc),
							   PGRAC_IC_HELLO_BYTES + ut_junk_a + 8192);
	UT_ASSERT(got >= 8192);
	/* Stream layout: [HELLO][junk 'J' ...][frame A 8192 x 'A'].  The final
	 * 8192 bytes must be exactly frame A. */
	for (i = got - 8192; i < got; i++)
		if (ut_acc[i] == 'A')
			a_seen++;
	UT_ASSERT_EQ(a_seen, 8192);
	UT_ASSERT(!cluster_ic_tier1_pending_outbound(UT_PEER_ID));
}

UT_TEST(test_smaller_frame_reuses_grown_buffer)
{
	static char frame_b[4096];
	ClusterICSendResult rc;

	memset(frame_b, 'B', sizeof(frame_b));
	ut_junk_b = ut_fill_until_eagain(ut_tx_fd);

	/* The dyn buffer capacity is now 8192 (frame A);  frame B is smaller.
	 * Pre-fix: the drain cursor (capacity - remaining) points 4096 bytes
	 * past B's tail start — stale frame-A bytes go on the wire. */
	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_b, sizeof(frame_b));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);
	UT_ASSERT(cluster_ic_tier1_pending_outbound(UT_PEER_ID));
}

UT_TEST(test_drain_delivers_frame_b_not_stale_a)
{
	long got;
	long i;
	long b_seen = 0;
	long stale_a = 0;

	got = ut_drain_and_collect(ut_rx_fd, ut_acc, (long)sizeof(ut_acc), ut_junk_b + 4096);
	UT_ASSERT(got >= 4096);
	for (i = got - 4096; i < got; i++) {
		if (ut_acc[i] == 'B')
			b_seen++;
		else if (ut_acc[i] == 'A')
			stale_a++; /* the pre-fix corruption signature */
	}
	UT_ASSERT_EQ(stale_a, 0);
	UT_ASSERT_EQ(b_seen, 4096);
}

UT_TEST(test_pending_cleared_and_counter_moved)
{
	UT_ASSERT(!cluster_ic_tier1_pending_outbound(UT_PEER_ID));
	/* This process runs as the CONTROL slot by default. */
	UT_ASSERT(cluster_ic_tier1_get_writable_drain(CLUSTER_IC_PLANE_CONTROL) > 0);
}

UT_TEST(test_close_peer_resets_queued_tail)
{
	static char frame_c[2048];
	ClusterICSendResult rc;

	memset(frame_c, 'C', sizeof(frame_c));
	(void)ut_fill_until_eagain(ut_tx_fd);
	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_c, sizeof(frame_c));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);
	UT_ASSERT(cluster_ic_tier1_pending_outbound(UT_PEER_ID));

	/* F4: the tail is a per-connection continuation — close must reset it
	 * so a reconnect can never start its stream with mid-frame bytes. */
	cluster_ic_tier1_close_peer(UT_PEER_ID, "test close");
	UT_ASSERT(!cluster_ic_tier1_pending_outbound(UT_PEER_ID));
}

UT_TEST(test_drain_on_dead_peer_hard_errors)
{
	UT_ASSERT(cluster_ic_tier1_drain_outbound(UT_PEER_ID) == CLUSTER_IC_SEND_HARD_ERROR);
	UT_ASSERT(cluster_ic_tier1_drain_outbound(-1) == CLUSTER_IC_SEND_HARD_ERROR);
	UT_ASSERT(cluster_ic_tier1_drain_outbound(CLUSTER_MAX_NODES) == CLUSTER_IC_SEND_HARD_ERROR);
}

/* ============================================================
 * GCS serve-stall round-5: multi-frame backpressure legs.
 *
 *	The single-tail state machine above proves ONE frame survives
 *	partial-IO.  The serve-stall root cause is the multi-frame case:
 *	while frame N's tail is still backpressured, a producer (GCS reply /
 *	forward / invalidate on the DATA plane; heartbeat / fanout on
 *	CONTROL) hands tier1 frame N+1.  Pre-fix tier1_send_bytes returned
 *	WOULD_BLOCK from the drain arm WITHOUT taking ownership of the new
 *	frame — every caller that treats WOULD_BLOCK as "transport retained
 *	the frame" (the documented L68 contract) silently lost it.  Under
 *	S3 load that is a lost GCS reply: the requester burns its full 5s
 *	reply wait + retransmit budget = the 33-54s stall wall.
 * ============================================================ */

/* T-9: bring the peer back up after T-7 closed it (fresh wire). */
UT_TEST(test_reconnect_after_close)
{
	int listener;
	int port;
	struct sockaddr_in sa;
	socklen_t salen = sizeof(sa);
	fd_set wfds;
	struct timeval tv;

	listener = ut_open_listener(&port);
	snprintf(ut_peer_info.interconnect_addr, sizeof(ut_peer_info.interconnect_addr), "127.0.0.1:%d",
			 port);

	UT_ASSERT(cluster_ic_tier1_connect_one(UT_PEER_ID, &ut_tx_fd));
	UT_ASSERT(ut_tx_fd >= 0);

	ut_rx_fd = accept(listener, (struct sockaddr *)&sa, &salen);
	UT_ASSERT(ut_rx_fd >= 0);
	FD_ZERO(&wfds);
	FD_SET(ut_tx_fd, &wfds);
	tv.tv_sec = 5;
	tv.tv_usec = 0;
	UT_ASSERT(select(ut_tx_fd + 1, NULL, &wfds, NULL, &tv) == 1);

	UT_ASSERT(cluster_ic_tier1_finish_connect(UT_PEER_ID, ut_tx_fd));
	UT_ASSERT(cluster_ic_tier1_hello_send_remaining(UT_PEER_ID) == 0);
	(void)close(listener);

	/* Drain the fresh-connection HELLO off the wire so the multi-frame
	 * legs below start from a clean, empty stream. */
	(void)ut_drain_all_and_sweep(UT_PEER_ID, ut_rx_fd, ut_acc, (long)sizeof(ut_acc));
}

/*
 * T-10 (RED core): a second whole frame handed to tier1 while the first
 * frame's tail is still backpressured must not be lost.  Pre-fix code
 * returned WOULD_BLOCK from the drain arm without copying/queueing frame
 * E — the stream then carried frame D only.
 */
UT_TEST(test_second_frame_survives_backpressure)
{
	static char frame_d[8192];
	static char frame_e[4096];
	ClusterICSendResult rc;
	long got;
	long i;
	long d_run = 0;
	long e_run = 0;

	memset(frame_d, 'D', sizeof(frame_d));
	memset(frame_e, 'E', sizeof(frame_e));

	(void)ut_fill_until_eagain(ut_tx_fd);

	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_d, sizeof(frame_d));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);
	UT_ASSERT(cluster_ic_tier1_pending_outbound(UT_PEER_ID));

	/* Frame E while D is pending: must be admitted (WOULD_BLOCK = the
	 * transport owns a copy) — never silently dropped. */
	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_e, sizeof(frame_e));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);

	got = ut_drain_all_and_sweep(UT_PEER_ID, ut_rx_fd, ut_acc, (long)sizeof(ut_acc));
	UT_ASSERT(got >= 12288);

	/* Stream tail must be exactly [D x 8192][E x 4096], in order. */
	for (i = got - 12288; i < got - 4096; i++)
		if (ut_acc[i] == 'D')
			d_run++;
	for (i = got - 4096; i < got; i++)
		if (ut_acc[i] == 'E')
			e_run++;
	UT_ASSERT_EQ(d_run, 8192);
	UT_ASSERT_EQ(e_run, 4096);
}

/*
 * T-11 (RED): several frames queued behind a backpressured tail must all
 * arrive, whole and in submission order (the per-peer FIFO contract every
 * GCS reply producer implicitly relies on).
 */
UT_TEST(test_fifo_preserves_multi_frame_order)
{
	static char frame_f[2048];
	static char frame_g[2048];
	static char frame_h[2048];
	ClusterICSendResult rc;
	long got;
	long i;
	long f_run = 0;
	long g_run = 0;
	long h_run = 0;

	memset(frame_f, 'F', sizeof(frame_f));
	memset(frame_g, 'G', sizeof(frame_g));
	memset(frame_h, 'H', sizeof(frame_h));

	(void)ut_fill_until_eagain(ut_tx_fd);

	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_f, sizeof(frame_f));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);
	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_g, sizeof(frame_g));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);
	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_h, sizeof(frame_h));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);

	got = ut_drain_all_and_sweep(UT_PEER_ID, ut_rx_fd, ut_acc, (long)sizeof(ut_acc));
	UT_ASSERT(got >= 6144);

	for (i = got - 6144; i < got - 4096; i++)
		if (ut_acc[i] == 'F')
			f_run++;
	for (i = got - 4096; i < got - 2048; i++)
		if (ut_acc[i] == 'G')
			g_run++;
	for (i = got - 2048; i < got; i++)
		if (ut_acc[i] == 'H')
			h_run++;
	UT_ASSERT_EQ(f_run, 2048);
	UT_ASSERT_EQ(g_run, 2048);
	UT_ASSERT_EQ(h_run, 2048);
}

/*
 * T-12: a backpressured peer must not block an idle peer (per-peer state
 * isolation — the tier1 half of the cross-peer HOL contract; the ring
 * half lives in test_cluster_lms_outbound).
 */
UT_TEST(test_backpressured_peer_does_not_block_other_peer)
{
	int listener;
	int port;
	struct sockaddr_in sa;
	socklen_t salen = sizeof(sa);
	fd_set wfds;
	struct timeval tv;
	static char frame_p[1024];
	static char frame_q[512];
	char rx2_buf[4096];
	long rx2_got = 0;
	int spins = 0;
	ClusterICSendResult rc;

	/* Bring up the second peer. */
	listener = ut_open_listener(&port);
	snprintf(ut_peer2_info.interconnect_addr, sizeof(ut_peer2_info.interconnect_addr),
			 "127.0.0.1:%d", port);
	ut_peer2_info.node_id = UT_PEER2_ID;
	ut_peer2_declared = true;

	UT_ASSERT(cluster_ic_tier1_connect_one(UT_PEER2_ID, &ut_tx2_fd));
	UT_ASSERT(ut_tx2_fd >= 0);
	ut_rx2_fd = accept(listener, (struct sockaddr *)&sa, &salen);
	UT_ASSERT(ut_rx2_fd >= 0);
	FD_ZERO(&wfds);
	FD_SET(ut_tx2_fd, &wfds);
	tv.tv_sec = 5;
	tv.tv_usec = 0;
	UT_ASSERT(select(ut_tx2_fd + 1, NULL, &wfds, NULL, &tv) == 1);
	UT_ASSERT(cluster_ic_tier1_finish_connect(UT_PEER2_ID, ut_tx2_fd));
	UT_ASSERT(cluster_ic_tier1_hello_send_remaining(UT_PEER2_ID) == 0);
	(void)close(listener);

	/* Backpressure peer 3 and park a frame on it. */
	memset(frame_p, 'P', sizeof(frame_p));
	memset(frame_q, 'Q', sizeof(frame_q));
	(void)ut_fill_until_eagain(ut_tx_fd);
	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_p, sizeof(frame_p));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);

	/* Peer 5 must still take a frame immediately. */
	rc = ClusterICOps_Tier1.send_bytes(UT_PEER2_ID, frame_q, sizeof(frame_q));
	UT_ASSERT(rc == CLUSTER_IC_SEND_DONE);
	UT_ASSERT(cluster_ic_tier1_pending_outbound(UT_PEER_ID));
	UT_ASSERT(!cluster_ic_tier1_pending_outbound(UT_PEER2_ID));

	/* Frame Q arrives on peer 5's wire (behind its HELLO). */
	for (;;) {
		ssize_t n
			= recv(ut_rx2_fd, rx2_buf + rx2_got, sizeof(rx2_buf) - (size_t)rx2_got, MSG_DONTWAIT);

		if (n > 0)
			rx2_got += (long)n;
		if (rx2_got >= PGRAC_IC_HELLO_BYTES + (long)sizeof(frame_q))
			break;
		usleep(1000);
		UT_ASSERT(++spins < 5000);
		if (spins >= 5000)
			break;
	}
	UT_ASSERT(rx2_got >= PGRAC_IC_HELLO_BYTES + (long)sizeof(frame_q));
	UT_ASSERT(rx2_buf[rx2_got - 1] == 'Q');

	/* Clean up peer 3's parked frame so the binary exits with an empty
	 * queue (drain to completion). */
	(void)ut_drain_all_and_sweep(UT_PEER_ID, ut_rx_fd, ut_acc, (long)sizeof(ut_acc));
}

/*
 * T-13: the FIFO is BOUNDED and refuses loudly at capacity — the caller
 * keeps ownership (NOT_ADMITTED + counter), the refused frame's bytes
 * never reach the wire, and everything that WAS admitted still arrives.
 */
UT_TEST(test_fifo_full_refuses_honestly)
{
	static char frame_t[1024];
	static char frame_z[64];
	static char frame_x[64];
	ClusterICSendResult rc;
	uint64 admitted0 = cluster_ic_tier1_get_fifo_admitted(CLUSTER_IC_PLANE_CONTROL);
	uint64 promoted0 = cluster_ic_tier1_get_fifo_promoted(CLUSTER_IC_PLANE_CONTROL);
	uint64 refused0 = cluster_ic_tier1_get_send_not_admitted(CLUSTER_IC_PLANE_CONTROL);
	long got;
	long i;
	long z_run = 0;
	long x_seen = 0;
	int q;

	memset(frame_t, 'T', sizeof(frame_t));
	memset(frame_z, 'Z', sizeof(frame_z));
	memset(frame_x, 'X', sizeof(frame_x));

	(void)ut_fill_until_eagain(ut_tx_fd);

	/* Tail frame parks in the single-tail buffer (not the FIFO). */
	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_t, sizeof(frame_t));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);

	/* Fill the FIFO to its frame cap — every one must be admitted. */
	for (q = 0; q < 2048; q++) {
		rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_z, sizeof(frame_z));
		UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK);
		if (rc != CLUSTER_IC_SEND_WOULD_BLOCK)
			return; /* avoid 2048 cascade failures */
	}
	UT_ASSERT_EQ((long)(cluster_ic_tier1_get_fifo_admitted(CLUSTER_IC_PLANE_CONTROL) - admitted0),
				 2048L);

	/* One more must be REFUSED — loudly, with the counter moving. */
	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_x, sizeof(frame_x));
	UT_ASSERT(rc == CLUSTER_IC_SEND_NOT_ADMITTED);
	UT_ASSERT(cluster_ic_tier1_get_send_not_admitted(CLUSTER_IC_PLANE_CONTROL) > refused0);

	/* Everything admitted still arrives, and no 'X' byte ever does. */
	got = ut_drain_all_and_sweep(UT_PEER_ID, ut_rx_fd, ut_acc, (long)sizeof(ut_acc));
	UT_ASSERT(got >= 1024 + 2048 * 64);
	for (i = got - 2048 * 64; i < got; i++)
		if (ut_acc[i] == 'Z')
			z_run++;
	for (i = 0; i < got; i++)
		if (ut_acc[i] == 'X')
			x_seen++;
	UT_ASSERT_EQ(z_run, 2048L * 64);
	UT_ASSERT_EQ(x_seen, 0);
	UT_ASSERT_EQ((long)(cluster_ic_tier1_get_fifo_promoted(CLUSTER_IC_PLANE_CONTROL) - promoted0),
				 2048L);
	UT_ASSERT(!cluster_ic_tier1_pending_outbound(UT_PEER_ID));
}

/*
 * T-14: queued whole frames are per-connection byte-stream continuations —
 * close_peer frees them all (counted, never silent) so a reconnect can
 * never replay stale frames onto the new stream.
 */
UT_TEST(test_close_peer_clears_fifo)
{
	static char frame_u[512];
	ClusterICSendResult rc;
	uint64 dropped0 = cluster_ic_tier1_get_fifo_dropped_close(CLUSTER_IC_PLANE_CONTROL);
	int q;

	memset(frame_u, 'U', sizeof(frame_u));
	(void)ut_fill_until_eagain(ut_tx_fd);

	rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_u, sizeof(frame_u));
	UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK); /* tail */
	for (q = 0; q < 2; q++) {
		rc = ClusterICOps_Tier1.send_bytes(UT_PEER_ID, frame_u, sizeof(frame_u));
		UT_ASSERT(rc == CLUSTER_IC_SEND_WOULD_BLOCK); /* FIFO */
	}

	cluster_ic_tier1_close_peer(UT_PEER_ID, "test close (fifo)");
	UT_ASSERT(!cluster_ic_tier1_pending_outbound(UT_PEER_ID));
	UT_ASSERT_EQ(
		(long)(cluster_ic_tier1_get_fifo_dropped_close(CLUSTER_IC_PLANE_CONTROL) - dropped0), 2L);
}

int
main(void)
{
	UT_PLAN(14);

	UT_RUN(test_connect_registers_peer_fd);
	UT_RUN(test_initial_eagain_queues_full_frame);
	UT_RUN(test_drain_delivers_frame_a_intact);
	UT_RUN(test_smaller_frame_reuses_grown_buffer);
	UT_RUN(test_drain_delivers_frame_b_not_stale_a);
	UT_RUN(test_pending_cleared_and_counter_moved);
	UT_RUN(test_close_peer_resets_queued_tail);
	UT_RUN(test_drain_on_dead_peer_hard_errors);
	UT_RUN(test_reconnect_after_close);
	UT_RUN(test_second_frame_survives_backpressure);
	UT_RUN(test_fifo_preserves_multi_frame_order);
	UT_RUN(test_backpressured_peer_does_not_block_other_peer);
	UT_RUN(test_fifo_full_refuses_honestly);
	UT_RUN(test_close_peer_clears_fifo);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
