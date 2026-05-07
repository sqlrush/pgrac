/*-------------------------------------------------------------------------
 *
 * cluster_ic.c
 *	  pgrac cluster internal IPC abstraction layer (Stage 0.18 stub
 *	  implementation).
 *
 *	  Stage 0.18 ships exactly one interconnect tier vtable -- the stub
 *	  -- whose send_bytes returns true for target == self and ereports
 *	  ERRCODE_FEATURE_NOT_SUPPORTED for cross-node, and whose
 *	  recv_bytes always returns false (no messages).  Tier1 (TCP)
 *	  lands in Stage 2 and replaces the vtable at startup based on the
 *	  cluster.interconnect_tier GUC; Tier2/3 (RDMA) land in Stage 6+.
 *
 *	  The high-level protocol layer (cluster_msg_*, cluster_rpc_*) is
 *	  fully wired here and forwards through the vtable: at stage 0.18
 *	  the only legal target is self, which exercises header
 *	  construction and CRC computation but produces no actual wire
 *	  traffic.  Stage 2 reuses these protocol-layer functions
 *	  unchanged once the vtable is swapped to TCP.
 *
 *	  See:
 *	    docs/cluster-ic-design.md         - design rationale, wire
 *	                                        format, Stage evolution
 *	    specs/spec-0.18-ic-framework.md   - this stage's spec
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ic.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All exported symbols use the cluster_ic_ / cluster_msg_ /
 *	  cluster_rpc_ / ClusterICOps prefix.  Excluded from disable-cluster
 *	  builds via cluster/Makefile (spec-0.3 symbol contract).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"

#include "cluster/cluster_ic.h"


/*
 * The four cluster_ic_mock_* SRFs are referenced unconditionally by
 * pg_proc.dat, so PG_FUNCTION_INFO_V1 must be emitted in both build
 * modes.  Bodies further below are #ifdef USE_PGRAC_CLUSTER guarded;
 * disable-cluster builds raise ERRCODE_FEATURE_NOT_SUPPORTED.
 */
PG_FUNCTION_INFO_V1(cluster_ic_mock_inject);
PG_FUNCTION_INFO_V1(cluster_ic_mock_drain_outbound);
PG_FUNCTION_INFO_V1(cluster_ic_mock_clear_all);
PG_FUNCTION_INFO_V1(cluster_ic_mock_recv_test);


#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_conf.h"	/* CLUSTER_MAX_NODES */
#include "cluster/cluster_guc.h"	/* cluster_node_id, cluster_interconnect_tier */
#include "cluster/cluster_inject.h" /* CLUSTER_INJECTION_POINT (stage 0.27) */


/*
 * Compile-time anchor for the wire format.  Mirrors the constant in
 * cluster_ic.h; if anyone changes ClusterMsgHeader without updating
 * the constant the build fails here.
 */
StaticAssertDecl(sizeof(ClusterMsgHeader) == PGRAC_IC_HEADER_BYTES,
				 "ClusterMsgHeader must be exactly PGRAC_IC_HEADER_BYTES");


/*
 * Active vtable.  Initialised to point at ClusterICOps_Stub by
 * cluster_ic_init (see below) so that the dereference path is always
 * valid; postmaster startup ordering guarantees cluster_ic_init runs
 * before any backend forks.
 */
const ClusterICOps *ClusterICOps_Active = NULL;


/* ============================================================
 * Stub vtable (Stage 0.18 default).
 * ============================================================ */

static bool
stub_send_bytes(int32 target_node_id, const void *buf pg_attribute_unused(),
				size_t len pg_attribute_unused())
{
	if (target_node_id == cluster_node_id) {
		/*
		 * Self-targeted send is a no-op success.  Stage 0 unit / single
		 * -node deployments need this path so callers can exercise the
		 * upper API without a listener; Stage 2+ TCP vtable will short-
		 * circuit self-target via local memory copy.
		 */
		return true;
	}

	if (cluster_node_id == -1) {
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("cluster.node_id is unconfigured (-1)"),
						errhint("Set cluster.node_id in postgresql.conf and restart.")));
	}

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cross-node interconnect is not available in stub tier"),
					errhint("Set cluster.interconnect_tier to tier1 (Stage 2+) for "
							"real cross-node IPC.")));

	return false; /* unreachable */
}

static bool
stub_recv_bytes(int32 *out_sender_node_id pg_attribute_unused(), void *buf pg_attribute_unused(),
				size_t bufsize pg_attribute_unused(),
				size_t *out_received_len pg_attribute_unused())
{
	/* No listener exists in stub mode; nothing is ever received. */
	return false;
}

static void
stub_tier_init(void)
{
	/* Stub: no resources to allocate. */
}

static void
stub_tier_shutdown(void)
{
	/* Stub: no resources to release. */
}

const ClusterICOps ClusterICOps_Stub = {
	.send_bytes = stub_send_bytes,
	.recv_bytes = stub_recv_bytes,
	.tier_init = stub_tier_init,
	.tier_shutdown = stub_tier_shutdown,
	.tier_name = "stub",
};


/* ============================================================
 * Tier selection (cluster_ic_init / shutdown).
 * ============================================================ */

void
cluster_ic_init(void)
{
	/*
	 * spec-2.1 Hardening v1.0.2 D-I1 defensive guard (extends F2; codex
	 * review P1 post-Sprint B): even if a future caller forgets the
	 * cluster_enabled gate at the cluster_init_shmem call site,
	 * cluster_ic_init itself must be a no-op when cluster.enabled=off.
	 * Per L15 (spec-1.16 v0.2): early-return alone is not enough --
	 * caller must also gate.  Both layers must hold (caller in
	 * cluster_shmem.c::cluster_init_shmem + this defensive guard) to
	 * prevent the same family bug as F2 (which was a v1.0.1 fix for
	 * cluster_conf_load).  Without this guard, cluster.enabled=off +
	 * cluster.interconnect_tier=tier1 would FATAL via the tier1 case
	 * below (tier1 raises ERRCODE_FEATURE_NOT_SUPPORTED until Stage 2
	 * lands), violating the vanilla PG path promise.
	 */
	if (!cluster_enabled)
		return;

	switch ((ClusterICTier)cluster_interconnect_tier) {
	case CLUSTER_IC_TIER_STUB:
		ClusterICOps_Active = &ClusterICOps_Stub;
		break;

	case CLUSTER_IC_TIER_MOCK:
		ClusterICOps_Active = &ClusterICOps_Mock;
		break;

	case CLUSTER_IC_TIER_1:
		/*
		 * spec-2.2 D2 -- tier1 vtable binding.  Real implementation
		 * lives in cluster_ic_tier1.c (spec-2.2 D3 NEW); until D3
		 * lands in this Sprint A round, the stub at the bottom of
		 * this file (look for "ClusterICOps_Tier1 stub" comment)
		 * provides linker-resolved function pointers that all
		 * ereport ERR_FEATURE_NOT_SUPPORTED with a clear D3-pending
		 * message.  Once D3 ships, the stub here is removed and the
		 * real const struct in cluster_ic_tier1.c takes over.
		 */
		ClusterICOps_Active = &ClusterICOps_Tier1;
		break;

	case CLUSTER_IC_TIER_2:
	case CLUSTER_IC_TIER_3:
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster.interconnect_tier=tier2/tier3 is not implemented"),
						errhint("tier2/tier3 (RDMA) land in Stage 6+; stay on stub for now.")));
		break;

	default:
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid cluster.interconnect_tier value: %d", cluster_interconnect_tier)));
		break;
	}

	Assert(ClusterICOps_Active != NULL);
	ClusterICOps_Active->tier_init();

	CLUSTER_INJECTION_POINT("cluster-ic-tier-selected");
}

void
cluster_ic_shutdown(void)
{
	if (ClusterICOps_Active != NULL) {
		ClusterICOps_Active->tier_shutdown();
		ClusterICOps_Active = NULL;
	}
}


/* ============================================================
 * Low-level byte stream API.  Pure forwarders to the active vtable;
 * upper layers should always dereference through ClusterICOps_Active
 * (not the stub instance directly) so Stage 2 swap takes effect.
 * ============================================================ */

bool
cluster_ic_send_bytes(int32 target_node_id, const void *buf, size_t len)
{
	Assert(ClusterICOps_Active != NULL);
	return ClusterICOps_Active->send_bytes(target_node_id, buf, len);
}

bool
cluster_ic_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize,
					  size_t *out_received_len)
{
	/*
	 * spec-2.2 D2 -- single-call pass-through with partial-OK semantics
	 * (kernel recv(2)-like).  Returns up to bufsize bytes from a SINGLE
	 * peer; caller may receive fewer than bufsize and must call again
	 * if a complete N-byte unit is required (use cluster_ic_recv_exact
	 * helper below for that).
	 *
	 * The vtable contract: recv_bytes returns true with *out_received >= 0
	 * (0 == no data ready under nonblocking semantics; > 0 == partial OR
	 * full read), or false on hard error.  Mock backend honours partial
	 * consumption via MockQueueEntry.consumed (spec-2.2 D2 P2-1 fix);
	 * tier1 backend honours kernel recv(2) short-read behaviour.
	 */
	Assert(ClusterICOps_Active != NULL);
	return ClusterICOps_Active->recv_bytes(out_sender_node_id, buf, bufsize,
										   out_received_len);
}


/*
 * spec-2.2 D2 / Q11=A / P2-1 fix -- recv_exact helper for callers that
 * MUST get a full N bytes from a single peer (e.g. wire-format readers
 * for the 24-byte ClusterMsgHeader / 36-byte ClusterICEnvelope / 64-byte
 * ClusterICHelloMsg).
 *
 * Unlike cluster_ic_recv_bytes which pass-throughs the underlying
 * vtable single-call (and may return partial on mock entry boundary or
 * TCP short read), this helper LOOPS until either:
 *   - total == bufsize (full read OK)
 *   - vtable returns false (hard error -- propagated)
 *   - vtable returns true with *out_received == 0 (EOF / no data;
 *     loop exits with total < bufsize, returns true with partial)
 *   - sender flip detected (the per-peer wire contract requires a
 *     single message be served by one peer; sender mismatch indicates
 *     either a wire-protocol bug OR a mock-test setup that injected
 *     interleaved senders -- callers wanting N bytes from ONE peer
 *     must reject this case).  In tier1 this never happens because
 *     each peer has its own dedicated socket; the mock backend's
 *     per-entry sender is preserved across partial chunks of the same
 *     entry, but a fresh head entry from a different sender will trip
 *     this guard, signaling the caller that the queue boundary fell
 *     in the middle of an N-byte read.
 *
 * Cross-ref: lessons L59 (PG TAP framework assumption) -- assuming
 * "single recv = full payload" is a wire-protocol assumption error
 * analogous to assuming "adjust_conf set-or-add"; the loop primitive
 * is the only correct one for length-prefixed wire formats.
 *
 * Returns true on success (with *out_received == bufsize) OR clean
 * EOF/no-data (with *out_received < bufsize).  Returns false on hard
 * error or sender flip mid-read.
 */
bool
cluster_ic_recv_exact(int32 *out_sender_node_id, void *buf, size_t bufsize,
					  size_t *out_received_len)
{
	size_t total = 0;
	char  *cursor = (char *) buf;
	int32  sender = -1;

	Assert(ClusterICOps_Active != NULL);

	if (out_received_len != NULL)
		*out_received_len = 0;
	if (out_sender_node_id != NULL)
		*out_sender_node_id = -1;

	while (total < bufsize)
	{
		size_t got = 0;
		int32  this_sender = -1;
		bool   ok;

		ok = ClusterICOps_Active->recv_bytes(&this_sender,
											 cursor + total,
											 bufsize - total,
											 &got);
		if (!ok)
		{
			/*
			 * Vtable signals "no data right now" (mock empty queue;
			 * tier1 EAGAIN; etc).  Treat as clean partial -- return
			 * what we have so far.  Caller distinguishes "got full N"
			 * vs "got partial" via *out_received_len comparison;
			 * partial is NOT an error here, just back-pressure for the
			 * caller's wait loop (LmonMain WaitEventSet for tier1).
			 */
			break;
		}
		if (got == 0)
			break; /* defensive: vtable signals no data via this path too */

		if (total == 0)
			sender = this_sender;
		else if (this_sender != sender)
		{
			/*
			 * Sender flip mid-read: queue boundary at a different peer.
			 * For a wire-format unit (header / envelope) this is a
			 * protocol violation.  Signal failure; caller must decide
			 * how to recover (typically: drop connection in tier1; in
			 * mock-tests this means the test setup is wrong).
			 */
			return false;
		}

		total += got;
	}

	if (out_received_len != NULL)
		*out_received_len = total;
	if (out_sender_node_id != NULL)
		*out_sender_node_id = sender;

	return true;
}


/* ============================================================
 * High-level protocol API.  Adds the ClusterMsgHeader plus CRC over
 * (header excl crc) + payload.  Stub mode still computes the CRC so
 * the protocol layer is exercised end-to-end during single-node
 * smoke testing -- this catches header-shape regressions early even
 * before Stage 2 ships real wire traffic.
 *
 * seq_no is a single global atomic counter for now.  Stage 2+ may
 * partition per target node for back-pressure / dedupe.
 * ============================================================ */

static uint32 ic_global_seq_no = 0;

static pg_crc32c
compute_msg_crc(const ClusterMsgHeader *hdr, const void *payload, uint32 payload_len)
{
	pg_crc32c crc;
	const uint8 *hdr_bytes = (const uint8 *)hdr;
	const size_t crc_offset = offsetof(ClusterMsgHeader, crc32);

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, hdr_bytes, crc_offset);
	if (payload_len > 0)
		COMP_CRC32C(crc, payload, payload_len);
	FIN_CRC32C(crc);
	return crc;
}

bool
cluster_msg_send(int32 target_node_id, uint16 msg_type, const void *payload, uint32 payload_len)
{
	ClusterMsgHeader hdr;

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = PGRAC_IC_MAGIC;
	hdr.protocol_version = PGRAC_IC_PROTOCOL_VERSION_V1;
	hdr.msg_type = msg_type;
	hdr.sender_node_id = (int16)cluster_node_id;
	hdr.seq_no = ++ic_global_seq_no;
	hdr.payload_len = payload_len;
	hdr.crc32 = compute_msg_crc(&hdr, payload, payload_len);

	if (!cluster_ic_send_bytes(target_node_id, &hdr, sizeof(hdr)))
		return false;

	if (payload_len > 0)
		return cluster_ic_send_bytes(target_node_id, payload, payload_len);

	return true;
}

bool
cluster_msg_recv(ClusterMsgHeader *out_hdr, void *payload_buf, uint32 payload_buf_size)
{
	int32 sender_node_id;
	size_t hdr_received;
	size_t payload_received;
	pg_crc32c expected_crc;

	if (!cluster_ic_recv_bytes(&sender_node_id, out_hdr, sizeof(*out_hdr), &hdr_received))
		return false;

	if (hdr_received != sizeof(*out_hdr)) {
		ereport(WARNING, (errmsg("cluster_msg_recv: short header read (%zu of %zu bytes)",
								 hdr_received, sizeof(*out_hdr))));
		return false;
	}

	if (out_hdr->magic != PGRAC_IC_MAGIC) {
		ereport(WARNING, (errmsg("cluster_msg_recv: bad magic 0x%08x (expected 0x%08x)",
								 out_hdr->magic, PGRAC_IC_MAGIC)));
		return false;
	}

	if (out_hdr->protocol_version != PGRAC_IC_PROTOCOL_VERSION_V1) {
		ereport(WARNING, (errmsg("cluster_msg_recv: unsupported protocol version %u",
								 out_hdr->protocol_version)));
		return false;
	}

	if (out_hdr->payload_len > payload_buf_size) {
		ereport(WARNING, (errmsg("cluster_msg_recv: payload %u exceeds buffer %u",
								 out_hdr->payload_len, payload_buf_size)));
		return false;
	}

	if (out_hdr->payload_len > 0) {
		if (!cluster_ic_recv_bytes(&sender_node_id, payload_buf, payload_buf_size,
								   &payload_received))
			return false;

		if (payload_received != out_hdr->payload_len) {
			ereport(WARNING, (errmsg("cluster_msg_recv: short payload read (%zu of %u bytes)",
									 payload_received, out_hdr->payload_len)));
			return false;
		}
	}

	expected_crc = compute_msg_crc(out_hdr, payload_buf, out_hdr->payload_len);
	if (!EQ_CRC32C(expected_crc, out_hdr->crc32)) {
		ereport(WARNING, (errmsg("cluster_msg_recv: CRC mismatch (got 0x%08x expected 0x%08x)",
								 out_hdr->crc32, expected_crc)));
		return false;
	}

	return true;
}

bool
cluster_rpc_call(int32 target_node_id, uint16 msg_type, const void *req, uint32 req_len,
				 void *resp_buf, uint32 resp_buf_size, uint32 *out_resp_len, int timeout_ms)
{
	ClusterMsgHeader hdr;
	uint32 sent_seq;
	int elapsed_ms = 0;
	const int poll_step_ms = 1;

	/*
	 * Capture the seq_no we are about to send so the receive loop can
	 * match the reply.  Stage 0.18 stub never produces a reply, so
	 * this loop simply runs until the timeout.
	 */
	sent_seq = ic_global_seq_no + 1;
	if (!cluster_msg_send(target_node_id, msg_type, req, req_len))
		return false;

	while (elapsed_ms < timeout_ms) {
		if (cluster_msg_recv(&hdr, resp_buf, resp_buf_size)) {
			if (hdr.seq_no == sent_seq) {
				if (out_resp_len != NULL)
					*out_resp_len = hdr.payload_len;
				return true;
			}
			/* Otherwise drop and keep waiting; Stage 2+ may queue. */
		}

		pg_usleep(poll_step_ms * 1000L);
		elapsed_ms += poll_step_ms;
	}

	return false; /* timeout */
}


/* ============================================================
 * Mock interconnect tier (Stage 0.26).
 *
 *	A second vtable that routes traffic through per-backend in-
 *	memory queues so multi-node test scenarios can be exercised
 *	from a single PG instance.  Only the SQL-level surface is
 *	intentionally exposed (the four cluster_ic_mock_* SRFs); all
 *	state lives in TopMemoryContext globals and is destroyed when
 *	the backend exits.
 * ============================================================ */

typedef struct MockQueueEntry {
	int32 sender_node_id;
	Size len;
	/*
	 * spec-2.2 D2 / Q11=A / P2-1 fix -- partial consumption support.
	 *
	 * Pre-spec-2.2 mock_recv_bytes always copied min(e->len, bufsize)
	 * and freed the entry, silently dropping the tail when bufsize <
	 * e->len.  This created an "every recv returns one full message"
	 * fiction that masked real partial-read behaviour and ABI errors
	 * which would surface only when tier1 (TCP) shipped.
	 *
	 * `consumed` records how many bytes have been delivered to recv
	 * callers so far; the entry is freed only when consumed == len.
	 * Callers that need full payload must use cluster_ic_recv_bytes
	 * (recv_exact loop) which iterates until total == bufsize OR
	 * EOF; mock now returns partial chunks honestly.
	 */
	Size consumed;
	uint8 *payload;
	struct MockQueueEntry *next;
} MockQueueEntry;

typedef struct MockQueue {
	MockQueueEntry *head;
	MockQueueEntry *tail;
	int count;
} MockQueue;

/*
 * Per-backend mock state.  Allocated lazily on first mock op
 * (avoids paying any memory cost for backends that never use
 * the mock tier).
 */
static MockQueue *mock_inbound_queue = NULL;
static MockQueue *mock_outbound_queues[CLUSTER_MAX_NODES] = { 0 };
static bool mock_state_initialised = false;

static void
mock_state_init(void)
{
	MemoryContext oldctx;

	if (mock_state_initialised)
		return;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	mock_inbound_queue = palloc0(sizeof(MockQueue));
	for (int i = 0; i < CLUSTER_MAX_NODES; i++)
		mock_outbound_queues[i] = palloc0(sizeof(MockQueue));
	MemoryContextSwitchTo(oldctx);

	mock_state_initialised = true;
}

static void
mock_queue_push(MockQueue *q, int32 sender, const void *buf, Size len)
{
	MemoryContext oldctx;
	MockQueueEntry *e;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	e = palloc0(sizeof(MockQueueEntry));
	e->sender_node_id = sender;
	e->len = len;
	if (len > 0) {
		e->payload = palloc(len);
		memcpy(e->payload, buf, len);
	}
	MemoryContextSwitchTo(oldctx);

	if (q->tail == NULL)
		q->head = e;
	else
		q->tail->next = e;
	q->tail = e;
	q->count++;
}

static MockQueueEntry *
mock_queue_pop(MockQueue *q)
{
	MockQueueEntry *e = q->head;

	if (e == NULL)
		return NULL;

	q->head = e->next;
	if (q->head == NULL)
		q->tail = NULL;
	q->count--;
	e->next = NULL;
	return e;
}

static void
mock_queue_free_entry(MockQueueEntry *e)
{
	if (e == NULL)
		return;
	if (e->payload != NULL)
		pfree(e->payload);
	pfree(e);
}

static void
mock_queue_drain(MockQueue *q)
{
	MockQueueEntry *e;

	while ((e = mock_queue_pop(q)) != NULL)
		mock_queue_free_entry(e);
}

static void
mock_require_mock_tier(const char *fname)
{
	if (cluster_interconnect_tier != CLUSTER_IC_TIER_MOCK)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("%s requires cluster.interconnect_tier = 'mock'", fname),
						errhint("Set cluster.interconnect_tier in postgresql.conf and restart.")));
}


/* vtable hooks ---------------------------------------------------- */

static bool
mock_send_bytes(int32 target_node_id, const void *buf, size_t len)
{
	CLUSTER_INJECTION_POINT("cluster-ic-mock-send-pre-enqueue");

	mock_state_init();

	if (target_node_id < 0 || target_node_id >= CLUSTER_MAX_NODES)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_ic mock send target_node_id %d out of range [0, %d)",
							   target_node_id, CLUSTER_MAX_NODES)));

	mock_queue_push(mock_outbound_queues[target_node_id], cluster_node_id, buf, len);
	return true;
}

static bool
mock_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize, size_t *out_received_len)
{
	MockQueueEntry *e;
	Size remaining;
	Size copy_len;

	mock_state_init();

	/*
	 * spec-2.2 D2 / Q11=A / P2-1 fix -- partial consumption.
	 *
	 * Look at queue head WITHOUT popping; only pop (and free) when
	 * the entry has been fully consumed.  This lets cluster_ic_recv_bytes
	 * (recv_exact loop) read a large entry across multiple bufsize-bound
	 * recv calls without losing the tail.
	 *
	 * Returns true (and *out_received_len > 0) on partial copy as well;
	 * caller's recv_exact loop will call back to drain the rest.
	 * Returns true with *out_received_len == 0 ONLY when the inbound
	 * queue is empty (signals "no data ready" to the recv_exact loop,
	 * which then exits the loop).
	 */
	e = mock_inbound_queue ? mock_inbound_queue->head : NULL;

	if (e == NULL)
	{
		/*
		 * Empty queue -- preserve PG-convention "false = no data
		 * available" (matches pre-spec-2.2 mock_recv_bytes contract).
		 * cluster_ic_recv_exact treats false from vtable as "stop
		 * loop with whatever we accumulated so far" rather than hard
		 * error; the existing cluster_ic_mock_recv_test SRF gates row
		 * emission on this bool return so empty queue => zero rows
		 * (014_ic_mock.pl tests 5 + 8 lock this).
		 */
		return false;
	}

	remaining = e->len - e->consumed;
	Assert(remaining > 0); /* fully-consumed entries must have been freed */

	copy_len = remaining < bufsize ? remaining : bufsize;
	if (copy_len > 0)
		memcpy(buf, e->payload + e->consumed, copy_len);
	e->consumed += copy_len;

	if (out_received_len != NULL)
		*out_received_len = copy_len;
	if (out_sender_node_id != NULL)
		*out_sender_node_id = e->sender_node_id;

	if (e->consumed == e->len)
	{
		/* Fully drained -- detach from queue and free. */
		MockQueueEntry *popped = mock_queue_pop(mock_inbound_queue);
		Assert(popped == e);
		mock_queue_free_entry(popped);
	}

	return true;
}

static void
mock_tier_init(void)
{
	mock_state_init();
}

static void
mock_tier_shutdown(void)
{
	if (!mock_state_initialised)
		return;

	mock_queue_drain(mock_inbound_queue);
	for (int i = 0; i < CLUSTER_MAX_NODES; i++)
		mock_queue_drain(mock_outbound_queues[i]);
}


const ClusterICOps ClusterICOps_Mock = {
	.send_bytes = mock_send_bytes,
	.recv_bytes = mock_recv_bytes,
	.tier_init = mock_tier_init,
	.tier_shutdown = mock_tier_shutdown,
	.tier_name = "mock",
};


/* ============================================================
 * spec-2.2 D2 -- ClusterICOps_Tier1 stub (transitional).
 *
 * Real Tier1 vtable lives in cluster_ic_tier1.c (spec-2.2 D3 NEW).
 * Until D3 lands in this Sprint A round, this stub provides
 * linker-resolved function pointers so cluster_unit (test_cluster_ic
 * test_tier1_vtable_extern_linkable) builds cleanly and so postmaster
 * with cluster.interconnect_tier=tier1 fails with a clear D3-pending
 * message at the FIRST send/recv call (rather than at link time).
 *
 * Once D3 ships the real const struct in cluster_ic_tier1.c, this
 * stub block must be DELETED -- otherwise the linker will report a
 * duplicate symbol.  D3's commit message must explicitly call out
 * the stub removal step.
 * ============================================================ */

static bool
tier1_stub_send_bytes(int32 target_node_id pg_attribute_unused(),
					  const void *buf pg_attribute_unused(),
					  size_t len pg_attribute_unused())
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster_ic tier1 send_bytes is not yet implemented"),
			 errhint("spec-2.2 D3 (cluster_ic_tier1.c NEW) lands later in "
					 "this Sprint A round; for now stay on tier=stub or "
					 "tier=mock for IC tests.")));
	return false; /* unreachable */
}

static bool
tier1_stub_recv_bytes(int32 *out_sender_node_id pg_attribute_unused(),
					  void *buf pg_attribute_unused(),
					  size_t bufsize pg_attribute_unused(),
					  size_t *out_received_len pg_attribute_unused())
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster_ic tier1 recv_bytes is not yet implemented"),
			 errhint("spec-2.2 D3 (cluster_ic_tier1.c NEW) lands later in "
					 "this Sprint A round.")));
	return false; /* unreachable */
}

static void
tier1_stub_tier_init(void)
{
	/*
	 * Per spec-2.2 §3.7 + §3.9 the caller (cluster_init_shmem) gates
	 * on cluster_enabled; cluster_ic_init has its own defensive guard
	 * (v1.0.2 D-I1).  This stub init is reached only when
	 * cluster.enabled=on AND cluster.interconnect_tier=tier1; it must
	 * not FATAL the postmaster (per §3.10 -- only listener bind failure
	 * may FATAL).  Emit a clear LOG instead so operators see the
	 * D3-pending status; first send/recv will then ERROR.
	 */
	ereport(LOG,
			(errmsg("cluster_ic tier1 stub active -- Sprint A D3 implementation pending"),
			 errhint("Postmaster will start; first cluster_ic_send_bytes / recv_bytes "
					 "call will ERROR until cluster_ic_tier1.c (spec-2.2 D3) ships.")));
}

static void
tier1_stub_tier_shutdown(void)
{
	/* No-op: stub allocated nothing. */
}

const ClusterICOps ClusterICOps_Tier1 = {
	.send_bytes = tier1_stub_send_bytes,
	.recv_bytes = tier1_stub_recv_bytes,
	.tier_init = tier1_stub_tier_init,
	.tier_shutdown = tier1_stub_tier_shutdown,
	.tier_name = "tier1-stub-pending-D3",
};

#endif /* USE_PGRAC_CLUSTER */


/* ============================================================
 * Mock SRFs (always-linked symbol surface).
 *
 *	Bodies are #ifdef USE_PGRAC_CLUSTER guarded; disable-cluster
 *	builds raise ERRCODE_FEATURE_NOT_SUPPORTED so SQL referring to
 *	these functions on a vanilla binary is rejected with a clear
 *	message rather than producing inscrutable link-time failures.
 * ============================================================ */

Datum
cluster_ic_mock_inject(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	int32 from_node = PG_GETARG_INT32(0);
	bytea *payload = PG_GETARG_BYTEA_PP(1);
	Size payload_len;
	const void *payload_data;

	mock_require_mock_tier("cluster_ic_mock_inject");

	payload_len = VARSIZE_ANY_EXHDR(payload);
	payload_data = VARDATA_ANY(payload);

	mock_state_init();
	mock_queue_push(mock_inbound_queue, from_node, payload_data, payload_len);

	PG_RETURN_VOID();
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_ic_mock_inject not available in --disable-cluster builds")));
	PG_RETURN_VOID();
#endif
}


Datum
cluster_ic_mock_drain_outbound(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	int32 target = PG_GETARG_INT32(0);
	ReturnSetInfo *rsinfo;
	MockQueueEntry *e;

	mock_require_mock_tier("cluster_ic_mock_drain_outbound");

	if (target < 0 || target >= CLUSTER_MAX_NODES)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("target_node_id %d out of range [0, %d)", target, CLUSTER_MAX_NODES)));

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	mock_state_init();

	while ((e = mock_queue_pop(mock_outbound_queues[target])) != NULL) {
		Datum values[2];
		bool nulls[2] = { false, false };
		bytea *payload_bytea;

		values[0] = Int32GetDatum(e->sender_node_id);

		payload_bytea = (bytea *)palloc(VARHDRSZ + e->len);
		SET_VARSIZE(payload_bytea, VARHDRSZ + e->len);
		if (e->len > 0)
			memcpy(VARDATA(payload_bytea), e->payload, e->len);
		values[1] = PointerGetDatum(payload_bytea);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

		mock_queue_free_entry(e);
	}

	return (Datum)0;
#else
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster_ic_mock_drain_outbound not available in --disable-cluster builds")));
	PG_RETURN_VOID();
#endif
}


Datum
cluster_ic_mock_clear_all(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	mock_require_mock_tier("cluster_ic_mock_clear_all");

	if (!mock_state_initialised)
		PG_RETURN_VOID();

	mock_queue_drain(mock_inbound_queue);
	for (int i = 0; i < CLUSTER_MAX_NODES; i++)
		mock_queue_drain(mock_outbound_queues[i]);

	PG_RETURN_VOID();
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_ic_mock_clear_all not available in --disable-cluster builds")));
	PG_RETURN_VOID();
#endif
}


Datum
cluster_ic_mock_recv_test(PG_FUNCTION_ARGS)
{
#ifdef USE_PGRAC_CLUSTER
	ReturnSetInfo *rsinfo;
	uint8 buf[8192];
	int32 sender = -1;
	size_t received = 0;

	mock_require_mock_tier("cluster_ic_mock_recv_test");

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (cluster_ic_recv_bytes(&sender, buf, sizeof(buf), &received)) {
		Datum values[2];
		bool nulls[2] = { false, false };
		bytea *payload_bytea;

		values[0] = Int32GetDatum(sender);

		payload_bytea = (bytea *)palloc(VARHDRSZ + received);
		SET_VARSIZE(payload_bytea, VARHDRSZ + received);
		if (received > 0)
			memcpy(VARDATA(payload_bytea), buf, received);
		values[1] = PointerGetDatum(payload_bytea);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum)0;
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_ic_mock_recv_test not available in --disable-cluster builds")));
	PG_RETURN_VOID();
#endif
}
