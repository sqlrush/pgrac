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
#define UT_LISTEN_BACKLOG 4

static ClusterNodeInfo ut_peer_info;
static bool ut_peer_declared = false;

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id)
{
	if (!ut_peer_declared || node_id != UT_PEER_ID)
		return NULL;
	return &ut_peer_info;
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

/* ============================================================
 * Tests.
 * ============================================================ */

static int ut_tx_fd = -1;			  /* tier1's registered peer fd (we hold a copy) */
static int ut_rx_fd = -1;			  /* our end of the wire */
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

int
main(void)
{
	UT_PLAN(8);

	UT_RUN(test_connect_registers_peer_fd);
	UT_RUN(test_initial_eagain_queues_full_frame);
	UT_RUN(test_drain_delivers_frame_a_intact);
	UT_RUN(test_smaller_frame_reuses_grown_buffer);
	UT_RUN(test_drain_delivers_frame_b_not_stale_a);
	UT_RUN(test_pending_cleared_and_counter_moved);
	UT_RUN(test_close_peer_resets_queued_tail);
	UT_RUN(test_drain_on_dead_peer_hard_errors);

	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
