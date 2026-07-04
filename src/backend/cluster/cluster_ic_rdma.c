/*-------------------------------------------------------------------------
 *
 * cluster_ic_rdma.c
 *	  RDMA provider, CM/QP data path, and tier2/tier3 vtables.
 *
 * Spec: spec-6.1-rdma-transport-stack.md
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ic_rdma.c
 *
 * NOTES
 *	  pgrac-original file.  Implements the spec-6.1 RDMA provider,
 *	  CM/QP ownership, CQE triage, and SEND-with-SGE scratch data path.
 *	  Live shared_buffers pages are never exposed for asynchronous RDMA DMA.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "port/pg_crc32c.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_ic_envelope.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_router.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_sf_dep.h"
#include "cluster/cluster_shmem.h"
#include "pgstat.h"
#include "utils/elog.h"
#include "utils/tuplestore.h"
#include "utils/wait_event.h"

#ifdef HAVE_LIBIBVERBS
#include <infiniband/verbs.h>
#endif
#if defined(HAVE_MLX5DV)
#include <infiniband/mlx5dv.h>
#endif
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
#include <netdb.h>
#include <rdma/rdma_cma.h>
#endif


#define PGRAC_CLUSTER_IC_RDMA_MAGIC ((uint32)0x414D4452)		 /* "RDMA" LE */
#define PGRAC_CLUSTER_IC_RDMA_PRIVATE_MAGIC ((uint32)0x52445056) /* "RDPR" LE */
#define PGRAC_CLUSTER_IC_RDMA_PRIVATE_VERSION 1
#define CLUSTER_IC_RDMA_PROVIDER_NAME_LEN 32
#define CLUSTER_IC_RDMA_ERRCODE_LEN 6
#define CLUSTER_IC_RDMA_ERRMSG_LEN 128
#define CLUSTER_IC_RDMA_MAX_SGE 8
#define CLUSTER_IC_RDMA_RECV_BUFFER_BYTES (PGRAC_IC_ENVELOPE_BYTES + PGRAC_IC_PAYLOAD_MAX)
#define CLUSTER_IC_RDMA_WR_TYPE_SEND UINT64CONST(0x5100000000000000)
#define CLUSTER_IC_RDMA_WR_TYPE_RECV UINT64CONST(0x5200000000000000)
#define CLUSTER_IC_RDMA_WR_TYPE_MASK UINT64CONST(0xFF00000000000000)
#define CLUSTER_IC_RDMA_WR_PEER_SHIFT 32
#define CLUSTER_IC_RDMA_CM_PRIVATE_BYTES sizeof(ClusterICRdmaPrivateData)

typedef struct ClusterICRdmaPeerStats {
	int32 node_id;
	int32 transport;
	int32 state;
	int32 cq_depth;
	char provider[CLUSTER_IC_RDMA_PROVIDER_NAME_LEN];
	char last_error_code[CLUSTER_IC_RDMA_ERRCODE_LEN];
	char last_error[CLUSTER_IC_RDMA_ERRMSG_LEN];
	pg_atomic_uint64 fallback_count;
	pg_atomic_uint64 send_count;
	pg_atomic_uint64 recv_count;
	pg_atomic_uint64 bytes_send;
	pg_atomic_uint64 bytes_recv;
	pg_atomic_uint64 latency_us_sum;
	pg_atomic_uint64 latency_sample_count;
} ClusterICRdmaPeerStats;

typedef struct ClusterICRdmaShmem {
	uint32 magic;
	int32 provider;
	int32 completion;
	int32 _pad;
	pg_atomic_uint32 mr_registered;
	pg_atomic_uint64 global_fallback_count;
	pg_atomic_uint64 block_sge_send_count;
	pg_atomic_uint64 block_sge_fallback_count;
	pg_atomic_uint64 tier3_send_count;
	pg_atomic_uint64 inline_send_count;
	pg_atomic_uint64 unsignaled_batch_count;
	pg_atomic_uint64 busypoll_us_burned;
	pg_atomic_uint64 busypoll_fallback_count;
	LWLockPadded lock;
	ClusterICRdmaPeerStats peers[CLUSTER_MAX_NODES];
} ClusterICRdmaShmem;

struct ClusterICRdmaCtx {
#ifdef HAVE_LIBIBVERBS
	struct ibv_context *verbs;
	struct ibv_pd *pd;
	struct ibv_cq *cq;
	struct ibv_comp_channel *completion_channel;
	struct ibv_mr *shared_mr;
#endif
	int unused;
};

struct ClusterICQp {
	int32 peer_id;
#ifdef HAVE_LIBIBVERBS
	struct ibv_qp *qp;
#endif
};

struct ClusterICWc {
	int status;
#ifdef HAVE_LIBIBVERBS
	struct ibv_wc wc;
#endif
};

typedef struct ClusterICRdmaInboundFrame {
	int32 peer_id;
	size_t len;
	size_t consumed;
	uint8 *data;
	struct ClusterICRdmaInboundFrame *next;
} ClusterICRdmaInboundFrame;

#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
typedef struct ClusterICRdmaPeer {
	int32 peer_id;
	bool active_side;
	bool connected;
	bool send_busy;
	struct rdma_cm_id *id;
	ClusterICQp qp;
	uint8 *send_buf;
	size_t send_buf_len;
	struct ibv_mr *send_mr;
	uint8 *recv_buf;
	size_t recv_buf_len;
	struct ibv_mr *recv_mr;
	uint8 *block_scratch_buf;
	size_t block_scratch_len;
	struct ibv_mr *block_scratch_mr;
	bool block_scratch_borrowed;
	uint8 *queued_buf;
	size_t queued_len;
	size_t queued_buf_len;
	uint32 wr_since_signal;
	uint32 signal_batch_k;
	ClusterICSgeReleaseCallback pending_release_cb[CLUSTER_IC_RDMA_MAX_SGE];
	void *pending_release_arg[CLUSTER_IC_RDMA_MAX_SGE];
	int pending_release_count;
} ClusterICRdmaPeer;
#endif

typedef struct ClusterICRdmaPrivateData {
	uint32 magic;
	uint16 version;
	uint16 rdma_port;
	uint32 rdma_pkey;
	uint32 rdma_qkey;
	uint8 hello[PGRAC_IC_HELLO_BYTES];
} ClusterICRdmaPrivateData;

static const char *RdmaUnavailableReason = "RDMA data path has not been initialized";
static ClusterICRdmaShmem *RdmaShmem = NULL;
static ClusterICRdmaCtx RdmaCtx;
static const ClusterICRdmaProvider *RdmaProvider = NULL;
static ClusterICRdmaInboundFrame *RdmaInboundHead = NULL;
static ClusterICRdmaInboundFrame *RdmaInboundTail = NULL;
#ifdef HAVE_LIBIBVERBS
static ClusterICMr RdmaSharedBuffersMr;
static bool RdmaCtxOpen = false;
static bool RdmaForkInitialized = false;
#endif
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
static struct rdma_event_channel *RdmaCmChannel = NULL;
static struct rdma_cm_id *RdmaListenId = NULL;
static ClusterICRdmaPeer RdmaPeers[CLUSTER_MAX_NODES];
#endif

static int rdma_provider_poll_cq(ClusterICRdmaCtx *ctx, ClusterICWc *out, int max);
#ifdef HAVE_LIBIBVERBS
static int rdma_busypoll_drain(ClusterICWc *wc, int max);
static void rdma_process_polled_completions(ClusterICWc *wc, int n);
#endif
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
static bool rdma_split_host_port(const char *addr, char *host, size_t host_len, char *port,
								 size_t port_len);
static void rdma_peer_fail_or_fallback(int32 peer_id, const char *reason);
#endif


static bool
rdma_valid_peer_id(int32 peer_id)
{
	return peer_id >= 0 && peer_id < CLUSTER_MAX_NODES;
}

static const char *
rdma_provider_name_from_guc(void)
{
	switch ((ClusterICRdmaProviderId)cluster_interconnect_rdma_provider) {
	case CLUSTER_IC_RDMA_PROVIDER_AUTO:
		return "auto";
	case CLUSTER_IC_RDMA_PROVIDER_VERBS:
		return ClusterICRdmaProvider_Verbs.name;
	case CLUSTER_IC_RDMA_PROVIDER_MLX5:
		return ClusterICRdmaProvider_Mlx5.name;
	}
	return "unknown";
}

static const char *
rdma_selected_provider_name(void)
{
	if (RdmaProvider != NULL)
		return RdmaProvider->name;
	return rdma_provider_name_from_guc();
}

static bool
rdma_mlx5_requested(void)
{
	return (ClusterICTier)cluster_interconnect_tier == CLUSTER_IC_TIER_3
		   || (ClusterICRdmaProviderId)cluster_interconnect_rdma_provider
				  == CLUSTER_IC_RDMA_PROVIDER_MLX5;
}

static bool
rdma_mlx5_build_available(const char **reason)
{
#ifdef HAVE_MLX5DV
	if (reason != NULL)
		*reason = NULL;
	return true;
#else
	if (reason != NULL)
		*reason = "binary was not built with mlx5dv support";
	return false;
#endif
}

static ClusterICRdmaPeerStats *
rdma_peer_stats(int32 peer_id)
{
	if (RdmaShmem == NULL || !rdma_valid_peer_id(peer_id))
		return NULL;
	return &RdmaShmem->peers[peer_id];
}

#ifdef HAVE_LIBIBVERBS
static uint64
rdma_make_wr_id(uint64 type, int32 peer_id)
{
	return type | (((uint64)(uint32)peer_id) << CLUSTER_IC_RDMA_WR_PEER_SHIFT);
}

static int32
rdma_wr_peer(uint64 wr_id)
{
	return (int32)((wr_id >> CLUSTER_IC_RDMA_WR_PEER_SHIFT) & UINT64CONST(0xFFFFFFFF));
}
#endif

#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
static void
rdma_inbound_enqueue(int32 peer_id, const void *data, size_t len)
{
	ClusterICRdmaInboundFrame *frame;
	MemoryContext oldctx;

	if (!rdma_valid_peer_id(peer_id) || data == NULL || len == 0)
		return;

	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	frame = (ClusterICRdmaInboundFrame *)palloc0(sizeof(*frame));
	frame->data = (uint8 *)palloc(len);
	memcpy(frame->data, data, len);
	frame->peer_id = peer_id;
	frame->len = len;
	frame->consumed = 0;
	frame->next = NULL;
	if (RdmaInboundTail != NULL)
		RdmaInboundTail->next = frame;
	else
		RdmaInboundHead = frame;
	RdmaInboundTail = frame;
	MemoryContextSwitchTo(oldctx);
}

static void
rdma_inbound_drop_peer(int32 peer_id)
{
	ClusterICRdmaInboundFrame *cur = RdmaInboundHead;
	ClusterICRdmaInboundFrame *prev = NULL;

	while (cur != NULL) {
		ClusterICRdmaInboundFrame *next = cur->next;

		if (cur->peer_id == peer_id) {
			if (prev != NULL)
				prev->next = next;
			else
				RdmaInboundHead = next;
			if (RdmaInboundTail == cur)
				RdmaInboundTail = prev;
			pfree(cur->data);
			pfree(cur);
		} else {
			prev = cur;
		}
		cur = next;
	}
}
#endif

static bool
rdma_inbound_read(int32 *out_sender_node_id, void *buf, size_t bufsize, size_t *out_received_len)
{
	ClusterICRdmaInboundFrame *frame = RdmaInboundHead;
	size_t avail;
	size_t nread;

	if (out_received_len != NULL)
		*out_received_len = 0;
	if (out_sender_node_id != NULL)
		*out_sender_node_id = -1;
	if (frame == NULL)
		return true;
	if (buf == NULL || bufsize == 0)
		return true;

	avail = frame->len - frame->consumed;
	nread = Min(bufsize, avail);
	memcpy(buf, frame->data + frame->consumed, nread);
	frame->consumed += nread;
	if (out_sender_node_id != NULL)
		*out_sender_node_id = frame->peer_id;
	if (out_received_len != NULL)
		*out_received_len = nread;

	if (frame->consumed == frame->len) {
		RdmaInboundHead = frame->next;
		if (RdmaInboundTail == frame)
			RdmaInboundTail = NULL;
		pfree(frame->data);
		pfree(frame);
	}
	return true;
}

#ifdef HAVE_LIBIBVERBS
static void
rdma_ctx_close(ClusterICRdmaCtx *ctx)
{
	if (ctx == NULL)
		return;

	if (ctx->shared_mr != NULL) {
		(void)ibv_dereg_mr(ctx->shared_mr);
		ctx->shared_mr = NULL;
	}
	if (ctx->cq != NULL) {
		(void)ibv_destroy_cq(ctx->cq);
		ctx->cq = NULL;
	}
	if (ctx->completion_channel != NULL) {
		(void)ibv_destroy_comp_channel(ctx->completion_channel);
		ctx->completion_channel = NULL;
	}
	if (ctx->pd != NULL) {
		(void)ibv_dealloc_pd(ctx->pd);
		ctx->pd = NULL;
	}
	if (ctx->verbs != NULL) {
		(void)ibv_close_device(ctx->verbs);
		ctx->verbs = NULL;
	}
	RdmaCtxOpen = false;
	memset(&RdmaSharedBuffersMr, 0, sizeof(RdmaSharedBuffersMr));
	cluster_ic_rdma_stats_note_mr_registered(false);
}

static bool
rdma_ctx_open_first_device(ClusterICRdmaCtx *ctx)
{
	struct ibv_device **devices;
	int ndev = 0;
	int cq_depth;

	if (ctx == NULL)
		return false;
	if (RdmaCtxOpen)
		return true;

	if (!RdmaForkInitialized) {
		if (ibv_fork_init() != 0) {
			RdmaUnavailableReason = "ibv_fork_init failed";
			return false;
		}
		RdmaForkInitialized = true;
	}

	devices = ibv_get_device_list(&ndev);
	if (devices == NULL || ndev <= 0) {
		if (devices != NULL)
			ibv_free_device_list(devices);
		RdmaUnavailableReason = "no RDMA HCA reported by libibverbs";
		return false;
	}

	memset(ctx, 0, sizeof(*ctx));
	ctx->verbs = ibv_open_device(devices[0]);
	ibv_free_device_list(devices);
	if (ctx->verbs == NULL) {
		RdmaUnavailableReason = "ibv_open_device failed";
		return false;
	}

	ctx->pd = ibv_alloc_pd(ctx->verbs);
	if (ctx->pd == NULL) {
		RdmaUnavailableReason = "ibv_alloc_pd failed";
		rdma_ctx_close(ctx);
		return false;
	}

	ctx->completion_channel = ibv_create_comp_channel(ctx->verbs);
	if (ctx->completion_channel == NULL) {
		RdmaUnavailableReason = "ibv_create_comp_channel failed";
		rdma_ctx_close(ctx);
		return false;
	}

	cq_depth = cluster_interconnect_rdma_max_send_wr * Max(cluster_conf_node_count(), 1) * 2;
	if (cq_depth < 64)
		cq_depth = 64;
	ctx->cq = ibv_create_cq(ctx->verbs, cq_depth, NULL, ctx->completion_channel, 0);
	if (ctx->cq == NULL) {
		RdmaUnavailableReason = "ibv_create_cq failed";
		rdma_ctx_close(ctx);
		return false;
	}
	if (ibv_req_notify_cq(ctx->cq, 0) != 0) {
		RdmaUnavailableReason = "ibv_req_notify_cq failed";
		rdma_ctx_close(ctx);
		return false;
	}

	RdmaCtxOpen = true;
	RdmaUnavailableReason = NULL;
	return true;
}

static bool
rdma_register_shared_buffers(ClusterICRdmaCtx *ctx, ClusterICMr *out)
{
	size_t len;
	int access;

	if (ctx == NULL || out == NULL || !RdmaCtxOpen || ctx->pd == NULL) {
		RdmaUnavailableReason = "RDMA protection domain is not initialized";
		return false;
	}
	if (BufferBlocks == NULL || NBuffers <= 0) {
		RdmaUnavailableReason = "shared_buffers memory is not initialized";
		return false;
	}
	if (ctx->shared_mr != NULL) {
		out->base = BufferBlocks;
		out->len = (size_t)NBuffers * (size_t)BLCKSZ;
		out->lkey = ctx->shared_mr->lkey;
		out->rkey = ctx->shared_mr->rkey;
		return true;
	}

	len = (size_t)NBuffers * (size_t)BLCKSZ;
	access = IBV_ACCESS_LOCAL_WRITE;
	ctx->shared_mr = ibv_reg_mr(ctx->pd, BufferBlocks, len, access);
	if (ctx->shared_mr == NULL) {
		RdmaUnavailableReason = "ibv_reg_mr(shared_buffers) failed";
		return false;
	}

	out->base = BufferBlocks;
	out->len = len;
	out->lkey = ctx->shared_mr->lkey;
	out->rkey = ctx->shared_mr->rkey;
	cluster_ic_rdma_stats_note_mr_registered(true);
	return true;
}
#endif

#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
static void
rdma_cm_id_set_peer(struct rdma_cm_id *id, int32 peer_id)
{
	if (id != NULL)
		id->context = (void *)(intptr_t)(peer_id + 1);
}

static int32
rdma_cm_id_peer(struct rdma_cm_id *id)
{
	intptr_t raw;

	if (id == NULL || id->context == NULL)
		return -1;
	raw = (intptr_t)id->context;
	return (int32)(raw - 1);
}

static void
rdma_build_private_data(ClusterICRdmaPrivateData *private_data)
{
	const char *cluster_name = "";
	const ClusterNodeInfo *self;

	if (ClusterConfShmem != NULL)
		cluster_name = ClusterConfShmem->cluster_name;

	memset(private_data, 0, sizeof(*private_data));
	private_data->magic = PGRAC_CLUSTER_IC_RDMA_PRIVATE_MAGIC;
	private_data->version = PGRAC_CLUSTER_IC_RDMA_PRIVATE_VERSION;
	self = cluster_conf_lookup_node(cluster_node_id);
	if (self != NULL) {
		private_data->rdma_port = (uint16)self->rdma_port;
		private_data->rdma_pkey = self->rdma_pkey;
		private_data->rdma_qkey = self->rdma_qkey;
	}
	cluster_ic_build_hello(private_data->hello, PGRAC_IC_HELLO_VERSION_V1,
						   PGRAC_IC_ENVELOPE_VERSION_V1, cluster_node_id, cluster_name);
}

static bool
rdma_verify_private_hello(const void *data, uint8 len, int32 expected_peer, int32 *out_peer_id,
						  const char **out_reason)
{
	const ClusterICRdmaPrivateData *private_data = (const ClusterICRdmaPrivateData *)data;
	ClusterICHelloMsg msg;
	const char *self_cluster_name;
	const ClusterNodeInfo *peer_conf;

	if (out_peer_id != NULL)
		*out_peer_id = -1;
	if (out_reason != NULL)
		*out_reason = "RDMA HELLO missing";

	if (data == NULL || len < sizeof(ClusterICRdmaPrivateData))
		return false;
	if (private_data->magic != PGRAC_CLUSTER_IC_RDMA_PRIVATE_MAGIC
		|| private_data->version != PGRAC_CLUSTER_IC_RDMA_PRIVATE_VERSION) {
		if (out_reason != NULL)
			*out_reason = "RDMA private data version mismatch";
		return false;
	}
	if (!cluster_ic_parse_hello(private_data->hello, &msg)) {
		if (out_reason != NULL)
			*out_reason = "RDMA HELLO bad magic";
		return false;
	}
	if (msg.hello_version != PGRAC_IC_HELLO_VERSION_V1
		|| msg.envelope_version != PGRAC_IC_ENVELOPE_VERSION_V1) {
		if (out_reason != NULL)
			*out_reason = "RDMA HELLO version mismatch";
		return false;
	}

	self_cluster_name = ClusterConfShmem != NULL ? ClusterConfShmem->cluster_name : "";
	if (ClusterConfShmem != NULL && strcmp(msg.cluster_name, self_cluster_name) != 0) {
		if (out_reason != NULL)
			*out_reason = "RDMA HELLO cluster_name mismatch";
		return false;
	}
	if (!rdma_valid_peer_id(msg.source_node_id)
		|| cluster_conf_lookup_node(msg.source_node_id) == NULL) {
		if (out_reason != NULL)
			*out_reason = "RDMA HELLO unknown source_node_id";
		return false;
	}
	if (expected_peer >= 0 && expected_peer != msg.source_node_id) {
		if (out_reason != NULL)
			*out_reason = "RDMA HELLO peer id mismatch";
		return false;
	}
	peer_conf = cluster_conf_lookup_node(msg.source_node_id);
	if (peer_conf == NULL) {
		if (out_reason != NULL)
			*out_reason = "RDMA HELLO unknown peer config";
		return false;
	}
	if (private_data->rdma_port != (uint16)peer_conf->rdma_port
		|| private_data->rdma_pkey != peer_conf->rdma_pkey
		|| private_data->rdma_qkey != peer_conf->rdma_qkey) {
		if (out_reason != NULL)
			*out_reason = "RDMA partition metadata mismatch";
		return false;
	}

	if (out_peer_id != NULL)
		*out_peer_id = msg.source_node_id;
	cluster_sf_note_peer_hello_capabilities(msg.source_node_id,
											cluster_ic_hello_capabilities(&msg));
	if (out_reason != NULL)
		*out_reason = NULL;
	return true;
}

static void
rdma_peer_release_buffers(ClusterICRdmaPeer *peer)
{
	if (peer == NULL)
		return;

	if (peer->send_mr != NULL) {
		(void)ibv_dereg_mr(peer->send_mr);
		peer->send_mr = NULL;
	}
	if (peer->recv_mr != NULL) {
		(void)ibv_dereg_mr(peer->recv_mr);
		peer->recv_mr = NULL;
	}
	if (peer->block_scratch_mr != NULL) {
		(void)ibv_dereg_mr(peer->block_scratch_mr);
		peer->block_scratch_mr = NULL;
	}
	if (peer->send_buf != NULL) {
		pfree(peer->send_buf);
		peer->send_buf = NULL;
		peer->send_buf_len = 0;
	}
	if (peer->recv_buf != NULL) {
		pfree(peer->recv_buf);
		peer->recv_buf = NULL;
		peer->recv_buf_len = 0;
	}
	if (peer->block_scratch_buf != NULL) {
		pfree(peer->block_scratch_buf);
		peer->block_scratch_buf = NULL;
		peer->block_scratch_len = 0;
		peer->block_scratch_borrowed = false;
	}
	if (peer->queued_buf != NULL) {
		pfree(peer->queued_buf);
		peer->queued_buf = NULL;
		peer->queued_buf_len = 0;
		peer->queued_len = 0;
	}
}

static void
rdma_peer_release_pending_send(ClusterICRdmaPeer *peer)
{
	int i;

	if (peer == NULL)
		return;

	for (i = 0; i < peer->pending_release_count; i++) {
		if (peer->pending_release_cb[i] != NULL)
			peer->pending_release_cb[i](peer->pending_release_arg[i]);
		peer->pending_release_cb[i] = NULL;
		peer->pending_release_arg[i] = NULL;
	}
	peer->pending_release_count = 0;
}

static bool
rdma_peer_add_pending_release(ClusterICRdmaPeer *peer, ClusterICSgeReleaseCallback cb, void *arg)
{
	int i;

	if (peer == NULL || cb == NULL)
		return true;

	for (i = 0; i < peer->pending_release_count; i++) {
		if (peer->pending_release_cb[i] == cb && peer->pending_release_arg[i] == arg)
			return true;
	}
	if (peer->pending_release_count >= CLUSTER_IC_RDMA_MAX_SGE)
		return false;

	i = peer->pending_release_count++;
	peer->pending_release_cb[i] = cb;
	peer->pending_release_arg[i] = arg;
	return true;
}

static void
rdma_peer_release_block_scratch(void *arg)
{
	ClusterICRdmaPeer *peer = (ClusterICRdmaPeer *)arg;

	if (peer != NULL)
		peer->block_scratch_borrowed = false;
}

static bool
rdma_peer_ensure_buffers(ClusterICRdmaPeer *peer)
{
	MemoryContext oldctx;

	if (peer == NULL || RdmaCtx.pd == NULL)
		return false;
	if (peer->send_buf != NULL && peer->recv_buf != NULL && peer->block_scratch_buf != NULL
		&& peer->send_mr != NULL && peer->recv_mr != NULL && peer->block_scratch_mr != NULL)
		return true;

	rdma_peer_release_buffers(peer);
	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	peer->send_buf_len = CLUSTER_IC_RDMA_RECV_BUFFER_BYTES;
	peer->recv_buf_len = CLUSTER_IC_RDMA_RECV_BUFFER_BYTES;
	peer->block_scratch_len = BLCKSZ;
	peer->send_buf = (uint8 *)palloc(peer->send_buf_len);
	peer->recv_buf = (uint8 *)palloc(peer->recv_buf_len);
	peer->block_scratch_buf = (uint8 *)palloc(peer->block_scratch_len);
	MemoryContextSwitchTo(oldctx);

	peer->send_mr
		= ibv_reg_mr(RdmaCtx.pd, peer->send_buf, peer->send_buf_len, IBV_ACCESS_LOCAL_WRITE);
	if (peer->send_mr == NULL) {
		RdmaUnavailableReason = "ibv_reg_mr(RDMA send scratch) failed";
		rdma_peer_release_buffers(peer);
		return false;
	}
	peer->recv_mr
		= ibv_reg_mr(RdmaCtx.pd, peer->recv_buf, peer->recv_buf_len, IBV_ACCESS_LOCAL_WRITE);
	if (peer->recv_mr == NULL) {
		RdmaUnavailableReason = "ibv_reg_mr(RDMA recv quarantine) failed";
		rdma_peer_release_buffers(peer);
		return false;
	}
	peer->block_scratch_mr = ibv_reg_mr(RdmaCtx.pd, peer->block_scratch_buf,
										peer->block_scratch_len, IBV_ACCESS_LOCAL_WRITE);
	if (peer->block_scratch_mr == NULL) {
		RdmaUnavailableReason = "ibv_reg_mr(RDMA block scratch) failed";
		rdma_peer_release_buffers(peer);
		return false;
	}
	cluster_ic_rdma_stats_note_mr_registered(true);
	return true;
}

static bool
rdma_peer_queue_send(ClusterICRdmaPeer *peer, const void *buf, size_t len)
{
	MemoryContext oldctx;

	if (peer == NULL || buf == NULL || len == 0 || len > CLUSTER_IC_RDMA_RECV_BUFFER_BYTES)
		return false;
	if (peer->queued_len > 0)
		return false;
	if (peer->queued_buf == NULL || peer->queued_buf_len < len) {
		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		if (peer->queued_buf != NULL)
			pfree(peer->queued_buf);
		peer->queued_buf = (uint8 *)palloc(len);
		peer->queued_buf_len = len;
		MemoryContextSwitchTo(oldctx);
	}
	memcpy(peer->queued_buf, buf, len);
	peer->queued_len = len;
	return true;
}

static void
rdma_peer_flush_queued_send(int32 peer_id)
{
	ClusterICRdmaPeer *peer;
	ClusterICSge sge;
	size_t len;
	bool inline_send;
	bool signaled;
	ClusterICSendResult rc;

	if (!rdma_valid_peer_id(peer_id))
		return;
	peer = &RdmaPeers[peer_id];
	if (!peer->connected || peer->send_busy || peer->queued_len == 0 || RdmaProvider == NULL)
		return;
	if (peer->queued_len > peer->send_buf_len || peer->send_mr == NULL || peer->qp.qp == NULL) {
		peer->queued_len = 0;
		rdma_peer_fail_or_fallback(peer_id, "RDMA queued send is no longer valid");
		return;
	}

	len = peer->queued_len;
	memcpy(peer->send_buf, peer->queued_buf, len);
	peer->queued_len = 0;
	memset(&sge, 0, sizeof(sge));
	sge.addr = peer->send_buf;
	sge.len = len;
	sge.lkey = peer->send_mr->lkey;
	inline_send = rdma_peer_inline_eligible(len);
	signaled = rdma_peer_next_send_signaled(peer, inline_send);

	pgstat_report_wait_start(WAIT_EVENT_INTERCONNECT_RDMA_SEND);
	rc = RdmaProvider->post_send(&peer->qp, &sge, 1, signaled, inline_send, 0);
	pgstat_report_wait_end();
	if (rc == CLUSTER_IC_SEND_DONE) {
		peer->send_busy = !inline_send;
		if (inline_send)
			rdma_stats_note_inline_send();
		cluster_ic_rdma_stats_note_send(peer_id, len, true);
	} else {
		cluster_ic_rdma_stats_note_error(
			peer_id, "58R16",
			RdmaUnavailableReason != NULL ? RdmaUnavailableReason : "RDMA queued post_send failed");
		rdma_peer_fail_or_fallback(peer_id, "RDMA queued post_send failed");
	}
}

static void
rdma_peer_close(int32 peer_id, const char *reason, bool fallback_to_tcp)
{
	ClusterICRdmaPeer *peer;

	if (!rdma_valid_peer_id(peer_id))
		return;
	peer = &RdmaPeers[peer_id];

	if (peer->id != NULL) {
		if (peer->connected)
			(void)rdma_disconnect(peer->id);
		if (peer->id->qp != NULL)
			rdma_destroy_qp(peer->id);
		rdma_destroy_id(peer->id);
	}
	peer->id = NULL;
	memset(&peer->qp, 0, sizeof(peer->qp));
	peer->qp.peer_id = peer_id;
	peer->connected = false;
	peer->send_busy = false;
	rdma_peer_release_pending_send(peer);
	rdma_peer_release_buffers(peer);
	rdma_inbound_drop_peer(peer_id);

	if (fallback_to_tcp) {
		cluster_ic_mux_set_peer_transport(peer_id, CLUSTER_IC_PEER_TRANSPORT_TCP,
										  CLUSTER_IC_RDMA_PEER_FALLBACK_TCP);
		cluster_ic_rdma_stats_note_fallback(peer_id,
											reason != NULL ? reason : "RDMA peer fell back to TCP");
	} else {
		cluster_ic_mux_set_peer_transport(peer_id, CLUSTER_IC_PEER_TRANSPORT_TCP,
										  CLUSTER_IC_RDMA_PEER_ERROR);
		cluster_ic_rdma_stats_note_error(peer_id, "58R16",
										 reason != NULL ? reason : "RDMA peer closed");
	}
}

static void
rdma_peer_fail_or_fallback(int32 peer_id, const char *reason)
{
	if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_OFF)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_IC_RDMA_FABRIC_ERROR),
						errmsg("RDMA peer %d failed and fallback is disabled", peer_id),
						errdetail("%s", reason != NULL ? reason : "unknown RDMA peer failure")));

	rdma_peer_close(peer_id, reason, true);
}

static bool
rdma_peer_inline_eligible(size_t frame_len)
{
	if (frame_len > PG_UINT32_MAX)
		return false;
	return cluster_ic_rdma_payload_inline_eligible((uint32)frame_len,
												  cluster_interconnect_rdma_inline_max);
}

static bool
rdma_peer_next_send_signaled(ClusterICRdmaPeer *peer, bool inline_send)
{
	if (peer == NULL || !inline_send)
		return true;
	if (peer->signal_batch_k == 0)
		peer->signal_batch_k = cluster_ic_rdma_signal_batch_k(
			(uint32)cluster_interconnect_rdma_max_send_wr);

	peer->wr_since_signal++;
	if (peer->wr_since_signal >= peer->signal_batch_k) {
		peer->wr_since_signal = 0;
		return true;
	}

	if (RdmaShmem != NULL)
		pg_atomic_fetch_add_u64(&RdmaShmem->unsignaled_batch_count, 1);
	return false;
}

static void
rdma_stats_note_inline_send(void)
{
	if (RdmaShmem != NULL)
		pg_atomic_fetch_add_u64(&RdmaShmem->inline_send_count, 1);
}

static void
rdma_stats_note_busypoll(uint64 burned_us, bool fallback)
{
	if (RdmaShmem == NULL)
		return;
	if (burned_us > 0)
		pg_atomic_fetch_add_u64(&RdmaShmem->busypoll_us_burned, burned_us);
	if (fallback)
		pg_atomic_fetch_add_u64(&RdmaShmem->busypoll_fallback_count, 1);
}

#ifdef HAVE_LIBIBVERBS
static bool
rdma_wc_status_is_peer_loss(int status)
{
	return status == IBV_WC_RNR_RETRY_EXC_ERR || status == IBV_WC_RETRY_EXC_ERR
		   || status == IBV_WC_WR_FLUSH_ERR;
}

static bool
rdma_wc_status_is_local_protection(int status)
{
	return status == IBV_WC_LOC_LEN_ERR || status == IBV_WC_LOC_QP_OP_ERR
		   || status == IBV_WC_LOC_EEC_OP_ERR || status == IBV_WC_LOC_PROT_ERR
		   || status == IBV_WC_MW_BIND_ERR || status == IBV_WC_BAD_RESP_ERR
		   || status == IBV_WC_LOC_ACCESS_ERR;
}

static void
rdma_handle_completion_error(int32 peer_id, uint64 wr_type, int status)
{
	char reason[96];

#if defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	if (wr_type == CLUSTER_IC_RDMA_WR_TYPE_SEND)
		rdma_peer_release_pending_send(&RdmaPeers[peer_id]);
#endif

	snprintf(reason, sizeof(reason), "RDMA completion error status %d", status);
	if (rdma_wc_status_is_local_protection(status)) {
		cluster_ic_rdma_stats_note_error(peer_id, "58R16", reason);
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_IC_RDMA_FABRIC_ERROR),
						errmsg("RDMA local protection completion error for peer %d", peer_id),
						errdetail("%s", reason)));
	}

	if (rdma_wc_status_is_peer_loss(status)) {
		cluster_ic_rdma_stats_note_error(peer_id, "08R04", reason);
		rdma_peer_fail_or_fallback(peer_id, reason);
		return;
	}

	cluster_ic_rdma_stats_note_error(peer_id, "58R16", reason);
	rdma_peer_fail_or_fallback(peer_id, reason);
}
#endif

static bool
rdma_create_cm_qp(ClusterICRdmaPeer *peer)
{
	struct ibv_qp_init_attr attr;

	if (peer == NULL || peer->id == NULL || RdmaCtx.pd == NULL || RdmaCtx.cq == NULL)
		return false;
	if (peer->id->qp != NULL) {
		peer->qp.peer_id = peer->peer_id;
		peer->qp.qp = peer->id->qp;
		return true;
	}

	memset(&attr, 0, sizeof(attr));
	attr.send_cq = RdmaCtx.cq;
	attr.recv_cq = RdmaCtx.cq;
	attr.qp_type = IBV_QPT_RC;
	attr.cap.max_send_wr = cluster_interconnect_rdma_max_send_wr;
	attr.cap.max_recv_wr = cluster_interconnect_rdma_max_send_wr;
	attr.cap.max_send_sge = CLUSTER_IC_RDMA_MAX_SGE + 1;
	attr.cap.max_recv_sge = 1;
	attr.cap.max_inline_data = cluster_interconnect_rdma_inline_max;

	if (rdma_create_qp(peer->id, RdmaCtx.pd, &attr) != 0) {
		RdmaUnavailableReason = "rdma_create_qp failed";
		return false;
	}
	peer->qp.peer_id = peer->peer_id;
	peer->qp.qp = peer->id->qp;
	return true;
}

static bool
rdma_post_peer_recv(ClusterICRdmaPeer *peer)
{
	ClusterICSge sge;

	if (peer == NULL || peer->qp.qp == NULL || peer->recv_buf == NULL || peer->recv_mr == NULL)
		return false;
	memset(&sge, 0, sizeof(sge));
	sge.addr = peer->recv_buf;
	sge.len = peer->recv_buf_len;
	sge.lkey = peer->recv_mr->lkey;
	return RdmaProvider != NULL && RdmaProvider->post_recv(&peer->qp, &sge);
}

static bool
rdma_prepare_peer_for_connect(int32 peer_id, struct rdma_cm_id *id, bool active_side)
{
	ClusterICRdmaPeer *peer;

	if (!rdma_valid_peer_id(peer_id) || id == NULL)
		return false;

	peer = &RdmaPeers[peer_id];
	if (peer->id != NULL && peer->id != id)
		rdma_peer_close(peer_id, "replacing duplicate RDMA CM id", false);

	memset(peer, 0, sizeof(*peer));
	peer->peer_id = peer_id;
	peer->active_side = active_side;
	peer->id = id;
	rdma_cm_id_set_peer(id, peer_id);

	if (!rdma_create_cm_qp(peer) || !rdma_peer_ensure_buffers(peer) || !rdma_post_peer_recv(peer)) {
		cluster_ic_rdma_stats_note_error(
			peer_id, "58R16",
			RdmaUnavailableReason != NULL ? RdmaUnavailableReason : "RDMA peer preparation failed");
		return false;
	}

	cluster_ic_mux_set_peer_transport(peer_id, CLUSTER_IC_PEER_TRANSPORT_TCP,
									  CLUSTER_IC_RDMA_PEER_CONNECTING);
	return true;
}

static bool
rdma_conn_params(struct rdma_conn_param *param, ClusterICRdmaPrivateData *private_data)
{
	if (param == NULL || private_data == NULL)
		return false;

	memset(param, 0, sizeof(*param));
	rdma_build_private_data(private_data);
	param->private_data = private_data;
	param->private_data_len = CLUSTER_IC_RDMA_CM_PRIVATE_BYTES;
	param->responder_resources = 1;
	param->initiator_depth = 1;
	param->retry_count = 7;
	param->rnr_retry_count = 7;
	return true;
}

static void
rdma_mark_peer_connected(int32 peer_id)
{
	ClusterICRdmaPeer *peer;

	if (!rdma_valid_peer_id(peer_id))
		return;
	peer = &RdmaPeers[peer_id];
	peer->connected = true;
	peer->send_busy = false;
	cluster_ic_mux_set_peer_transport(peer_id, CLUSTER_IC_PEER_TRANSPORT_RDMA,
									  CLUSTER_IC_RDMA_PEER_CONNECTED);
	ereport(LOG, (errmsg("cluster_ic RDMA peer %d connected", peer_id)));
}

static bool
rdma_peer_addr(int32 peer_id, const char **addr, const char **reason)
{
	const ClusterNodeInfo *n;

	if (addr != NULL)
		*addr = NULL;
	if (reason != NULL)
		*reason = NULL;
	n = cluster_conf_lookup_node(peer_id);
	if (n == NULL) {
		if (reason != NULL)
			*reason = "peer is not declared in pgrac.conf";
		return false;
	}
	if (n->rdma_addr[0] == '\0') {
		if (reason != NULL)
			*reason = "peer has no rdma_addr in pgrac.conf";
		return false;
	}
	if (addr != NULL)
		*addr = n->rdma_addr;
	return true;
}

static void
rdma_start_active_connect_one(int32 peer_id)
{
	const char *addr = NULL;
	const char *reason = NULL;
	char host[CLUSTER_NODE_ADDR_LEN];
	char port[16];
	struct addrinfo hints;
	struct addrinfo *res = NULL;
	struct rdma_cm_id *id = NULL;
	int gai_rc;

	if (!rdma_valid_peer_id(peer_id))
		return;
	if (RdmaPeers[peer_id].id != NULL || RdmaPeers[peer_id].connected)
		return;
	if (!rdma_peer_addr(peer_id, &addr, &reason)) {
		rdma_peer_fail_or_fallback(peer_id, reason);
		return;
	}
	if (!rdma_split_host_port(addr, host, sizeof(host), port, sizeof(port))) {
		rdma_peer_fail_or_fallback(peer_id, "peer rdma_addr is malformed");
		return;
	}
	if (rdma_create_id(RdmaCmChannel, &id, NULL, RDMA_PS_TCP) != 0) {
		rdma_peer_fail_or_fallback(peer_id, "rdma_create_id(active) failed");
		return;
	}
	rdma_cm_id_set_peer(id, peer_id);

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	gai_rc = getaddrinfo(host, port, &hints, &res);
	if (gai_rc != 0 || res == NULL) {
		rdma_destroy_id(id);
		rdma_peer_fail_or_fallback(peer_id, "getaddrinfo(peer RDMA address) failed");
		return;
	}

	if (rdma_resolve_addr(id, NULL, res->ai_addr, cluster_interconnect_connect_timeout_ms) != 0) {
		freeaddrinfo(res);
		rdma_destroy_id(id);
		rdma_peer_fail_or_fallback(peer_id, "rdma_resolve_addr failed");
		return;
	}
	freeaddrinfo(res);

	RdmaPeers[peer_id].peer_id = peer_id;
	RdmaPeers[peer_id].active_side = true;
	RdmaPeers[peer_id].id = id;
	cluster_ic_mux_set_peer_transport(peer_id, CLUSTER_IC_PEER_TRANSPORT_TCP,
									  CLUSTER_IC_RDMA_PEER_CONNECTING);
}

static void
rdma_start_active_connects(void)
{
	int32 self_id = cluster_node_id;
	int32 peer;

	for (peer = 0; peer < CLUSTER_MAX_NODES; peer++) {
		if (peer == self_id || cluster_conf_lookup_node(peer) == NULL)
			continue;
		if (cluster_ic_mesh_role_for_pair(self_id, peer) == CLUSTER_IC_MESH_ACTIVE)
			rdma_start_active_connect_one(peer);
	}
}

static void
rdma_process_recv_completion(ClusterICRdmaPeer *peer, uint32 byte_len)
{
	if (peer == NULL || byte_len == 0 || byte_len > peer->recv_buf_len) {
		if (peer != NULL)
			rdma_peer_fail_or_fallback(peer->peer_id, "RDMA receive completion length invalid");
		return;
	}

	rdma_inbound_enqueue(peer->peer_id, peer->recv_buf, byte_len);
	cluster_ic_rdma_stats_note_recv(peer->peer_id, byte_len, true);
	if (!rdma_post_peer_recv(peer))
		rdma_peer_fail_or_fallback(peer->peer_id, RdmaUnavailableReason != NULL
													  ? RdmaUnavailableReason
													  : "RDMA recv repost failed");
}

static void
rdma_dispatch_pending_frames(void)
{
	for (;;) {
		ClusterICEnvelope env;
		int32 sender = -1;
		size_t got = 0;
		uint8 *payload = NULL;
		ClusterICEnvelopeVerifyResult vrc;

		if (!cluster_ic_recv_exact(&sender, &env, sizeof(env), &got))
			return;
		if (got == 0)
			return;
		if (got != sizeof(env)) {
			cluster_ic_rdma_stats_note_error(sender, "08P01", "short RDMA envelope");
			rdma_peer_fail_or_fallback(sender, "short RDMA envelope");
			return;
		}
		if (env.payload_length > PGRAC_IC_PAYLOAD_MAX) {
			cluster_ic_rdma_stats_note_error(sender, "08P01", "oversized RDMA envelope payload");
			rdma_peer_fail_or_fallback(sender, "oversized RDMA envelope payload");
			return;
		}
		if (env.payload_length > 0) {
			payload = (uint8 *)palloc(env.payload_length);
			if (!cluster_ic_recv_exact(&sender, payload, env.payload_length, &got)) {
				pfree(payload);
				return;
			}
			if (got != env.payload_length) {
				pfree(payload);
				cluster_ic_rdma_stats_note_error(sender, "08P01", "short RDMA payload");
				rdma_peer_fail_or_fallback(sender, "short RDMA payload");
				return;
			}
		}

		vrc = cluster_ic_envelope_verify(&env, payload, env.payload_length, (uint32)cluster_node_id,
										 sender);
		if (vrc == CLUSTER_IC_ENVELOPE_OK) {
			if (!cluster_ic_dispatch_envelope(&env, payload, sender)) {
				cluster_ic_rdma_stats_note_error(sender, "08P01",
												 "RDMA envelope dispatch rejected msg_type");
				rdma_peer_fail_or_fallback(sender, "RDMA envelope dispatch rejected msg_type");
			}
		} else if (vrc == CLUSTER_IC_ENVELOPE_DROP_NO_CLOSE) {
			cluster_ic_rdma_stats_note_error(sender, "53R20",
											 "RDMA envelope dropped by epoch guard");
		} else {
			cluster_ic_rdma_stats_note_error(sender, "08P01", "RDMA envelope verification failed");
			rdma_peer_fail_or_fallback(sender, "RDMA envelope verification failed");
		}

		if (payload != NULL)
			pfree(payload);
	}
}

static ClusterICSendResult
rdma_peer_post_sge(int32 peer_id, const ClusterICEnvelope *env, const ClusterICSge *payload_sge,
				   int n_sge, uint32 payload_len)
{
	ClusterICRdmaPeer *peer;
	ClusterICSge send_sge[CLUSTER_IC_RDMA_MAX_SGE + 1];
	ClusterICSgeReleaseCallback release_cb[CLUSTER_IC_RDMA_MAX_SGE];
	void *release_arg[CLUSTER_IC_RDMA_MAX_SGE];
	int send_sge_count = 0;
	int release_count = 0;
	uint8 *scratch;
	size_t scratch_len = 0;
	bool saw_registered_payload = false;
	bool inline_send = false;
	bool signaled = true;
	int i;
	ClusterICSendResult rc;

	if (!rdma_valid_peer_id(peer_id))
		return CLUSTER_IC_SEND_HARD_ERROR;
	peer = &RdmaPeers[peer_id];
	if (!peer->connected || peer->qp.qp == NULL || peer->send_mr == NULL)
		return CLUSTER_IC_SEND_HARD_ERROR;
	if (peer->send_busy)
		return CLUSTER_IC_SEND_WOULD_BLOCK;
	if (sizeof(*env) + payload_len > peer->send_buf_len)
		return CLUSTER_IC_SEND_HARD_ERROR;

	memset(send_sge, 0, sizeof(send_sge));
	memcpy(peer->send_buf, env, sizeof(*env));
	send_sge[send_sge_count].addr = peer->send_buf;
	send_sge[send_sge_count].len = sizeof(*env);
	send_sge[send_sge_count].lkey = peer->send_mr->lkey;
	send_sge_count++;

	scratch = peer->send_buf + sizeof(*env);
	for (i = 0; i < n_sge; i++) {
		if (payload_sge[i].len == 0)
			continue;
		if (payload_sge[i].release_cb != NULL) {
			bool duplicate = false;
			int j;

			for (j = 0; j < release_count; j++) {
				if (release_cb[j] == payload_sge[i].release_cb
					&& release_arg[j] == payload_sge[i].release_arg) {
					duplicate = true;
					break;
				}
			}
			if (!duplicate) {
				if (release_count >= lengthof(release_cb))
					return CLUSTER_IC_SEND_HARD_ERROR;
				release_cb[release_count] = payload_sge[i].release_cb;
				release_arg[release_count] = payload_sge[i].release_arg;
				release_count++;
			}
		}
		if (payload_sge[i].lkey != 0) {
			if (send_sge_count >= lengthof(send_sge))
				return CLUSTER_IC_SEND_HARD_ERROR;
			send_sge[send_sge_count] = payload_sge[i];
			send_sge_count++;
			saw_registered_payload = true;
			continue;
		}

		/*
		 * Keep payload order without building a scatter/gather reorder engine:
		 * unregistered payload is allowed as a prefix (for example the block
		 * reply header before the shared_buffers page).  If a later
		 * unregistered segment appears after a registered SGE, fall back to the
		 * contiguous safe path at the caller.
		 */
		if (saw_registered_payload)
			return CLUSTER_IC_SEND_HARD_ERROR;
		memcpy(scratch + scratch_len, payload_sge[i].addr, payload_sge[i].len);
		scratch_len += payload_sge[i].len;
	}

	if (scratch_len > 0) {
		int insert_at = 1;

		if (send_sge_count >= lengthof(send_sge))
			return CLUSTER_IC_SEND_HARD_ERROR;
		memmove(&send_sge[insert_at + 1], &send_sge[insert_at],
				sizeof(send_sge[0]) * (send_sge_count - insert_at));
		send_sge[insert_at].addr = scratch;
		send_sge[insert_at].len = scratch_len;
		send_sge[insert_at].lkey = peer->send_mr->lkey;
		send_sge_count++;
	}

	inline_send = !saw_registered_payload && rdma_peer_inline_eligible(sizeof(*env) + payload_len);
	signaled = rdma_peer_next_send_signaled(peer, inline_send);

	pgstat_report_wait_start(WAIT_EVENT_INTERCONNECT_RDMA_SEND);
	rc = RdmaProvider->post_send(&peer->qp, send_sge, send_sge_count, signaled, inline_send, 0);
	pgstat_report_wait_end();
	if (rc == CLUSTER_IC_SEND_DONE) {
		int j;

		if (inline_send) {
			for (j = 0; j < release_count; j++)
				release_cb[j](release_arg[j]);
			rdma_stats_note_inline_send();
		} else {
			rdma_peer_release_pending_send(peer);
			for (j = 0; j < release_count; j++) {
				if (!rdma_peer_add_pending_release(peer, release_cb[j], release_arg[j]))
					ereport(FATAL, (errcode(ERRCODE_CLUSTER_IC_RDMA_FABRIC_ERROR),
									errmsg("RDMA SEND posted but release callback table is full"),
									errdetail("peer_id=%d, release_count=%d", peer_id, release_count)));
			}
		}
		peer->send_busy = !inline_send;
		cluster_ic_rdma_stats_note_send(peer_id, sizeof(*env) + payload_len, true);
	} else if (rc == CLUSTER_IC_SEND_HARD_ERROR) {
		cluster_ic_rdma_stats_note_error(peer_id, "58R16",
										 RdmaUnavailableReason != NULL ? RdmaUnavailableReason
																	   : "RDMA post_send failed");
	}
	return rc;
}

static ClusterICSendResult
rdma_peer_send_bytes(int32 peer_id, const void *buf, size_t len)
{
	ClusterICRdmaPeer *peer;
	ClusterICSge sge;
	bool inline_send;
	bool signaled;
	ClusterICSendResult rc;

	if (!rdma_valid_peer_id(peer_id) || buf == NULL || len == 0)
		return CLUSTER_IC_SEND_HARD_ERROR;
	peer = &RdmaPeers[peer_id];
	if (!peer->connected || peer->send_mr == NULL || peer->qp.qp == NULL)
		return CLUSTER_IC_SEND_HARD_ERROR;
	if (len > peer->send_buf_len)
		return CLUSTER_IC_SEND_HARD_ERROR;
	if (peer->send_busy) {
		if (rdma_peer_queue_send(peer, buf, len))
			return CLUSTER_IC_SEND_WOULD_BLOCK;
		cluster_ic_rdma_stats_note_error(peer_id, "58R16", "RDMA outbound queue is full");
		return CLUSTER_IC_SEND_HARD_ERROR;
	}

	memcpy(peer->send_buf, buf, len);
	memset(&sge, 0, sizeof(sge));
	sge.addr = peer->send_buf;
	sge.len = len;
	sge.lkey = peer->send_mr->lkey;
	inline_send = rdma_peer_inline_eligible(len);
	signaled = rdma_peer_next_send_signaled(peer, inline_send);

	pgstat_report_wait_start(WAIT_EVENT_INTERCONNECT_RDMA_SEND);
	rc = RdmaProvider->post_send(&peer->qp, &sge, 1, signaled, inline_send, 0);
	pgstat_report_wait_end();
	if (rc == CLUSTER_IC_SEND_DONE) {
		peer->send_busy = !inline_send;
		if (inline_send)
			rdma_stats_note_inline_send();
		cluster_ic_rdma_stats_note_send(peer_id, len, true);
	} else if (rc == CLUSTER_IC_SEND_HARD_ERROR) {
		cluster_ic_rdma_stats_note_error(peer_id, "58R16",
										 RdmaUnavailableReason != NULL ? RdmaUnavailableReason
																	   : "RDMA post_send failed");
	}
	return rc;
}
#endif

static void
rdma_stats_note_tier3_send(void)
{
	if (RdmaShmem != NULL
		&& ((ClusterICTier)cluster_interconnect_tier == CLUSTER_IC_TIER_3
			|| (ClusterICRdmaProviderId)cluster_interconnect_rdma_provider
				   == CLUSTER_IC_RDMA_PROVIDER_MLX5))
		pg_atomic_fetch_add_u64(&RdmaShmem->tier3_send_count, 1);
}

static Size
cluster_ic_rdma_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterICRdmaShmem));
}

static void
cluster_ic_rdma_shmem_init(void)
{
	bool found;

	RdmaShmem = (ClusterICRdmaShmem *)ShmemInitStruct("pgrac cluster_ic_rdma",
													  cluster_ic_rdma_shmem_size(), &found);
	if (!found) {
		int i;

		memset(RdmaShmem, 0, sizeof(*RdmaShmem));
		RdmaShmem->magic = PGRAC_CLUSTER_IC_RDMA_MAGIC;
		RdmaShmem->provider = cluster_interconnect_rdma_provider;
		RdmaShmem->completion = cluster_interconnect_rdma_completion;
		LWLockInitialize(&RdmaShmem->lock.lock, LWTRANCHE_CLUSTER_IC_RDMA);
		pg_atomic_init_u32(&RdmaShmem->mr_registered, 0);
		pg_atomic_init_u64(&RdmaShmem->global_fallback_count, 0);
		pg_atomic_init_u64(&RdmaShmem->block_sge_send_count, 0);
		pg_atomic_init_u64(&RdmaShmem->block_sge_fallback_count, 0);
		pg_atomic_init_u64(&RdmaShmem->tier3_send_count, 0);
		pg_atomic_init_u64(&RdmaShmem->inline_send_count, 0);
		pg_atomic_init_u64(&RdmaShmem->unsignaled_batch_count, 0);
		pg_atomic_init_u64(&RdmaShmem->busypoll_us_burned, 0);
		pg_atomic_init_u64(&RdmaShmem->busypoll_fallback_count, 0);
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			ClusterICRdmaPeerStats *p = &RdmaShmem->peers[i];

			p->node_id = i;
			p->transport = CLUSTER_IC_PEER_TRANSPORT_TCP;
			p->state = CLUSTER_IC_RDMA_PEER_DISABLED;
			p->cq_depth = 0;
			strlcpy(p->provider, rdma_selected_provider_name(), sizeof(p->provider));
			pg_atomic_init_u64(&p->fallback_count, 0);
			pg_atomic_init_u64(&p->send_count, 0);
			pg_atomic_init_u64(&p->recv_count, 0);
			pg_atomic_init_u64(&p->bytes_send, 0);
			pg_atomic_init_u64(&p->bytes_recv, 0);
			pg_atomic_init_u64(&p->latency_us_sum, 0);
			pg_atomic_init_u64(&p->latency_sample_count, 0);
		}
	}
}

static const ClusterShmemRegion cluster_ic_rdma_region = {
	.name = "pgrac cluster_ic_rdma",
	.size_fn = cluster_ic_rdma_shmem_size,
	.init_fn = cluster_ic_rdma_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_ic_rdma",
	.reserved_flags = 0,
};

void
cluster_ic_rdma_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ic_rdma_region);
}


bool
cluster_ic_rdma_runtime_available(const char **reason)
{
#ifdef HAVE_LIBIBVERBS
	struct ibv_device **devices;
	int ndev = 0;

	if (RdmaCtxOpen) {
		if (reason != NULL)
			*reason = NULL;
		return true;
	}

	devices = ibv_get_device_list(&ndev);
	if (devices != NULL && ndev > 0) {
		ibv_free_device_list(devices);
		if (reason != NULL)
			*reason = NULL;
		return true;
	}

	if (devices != NULL)
		ibv_free_device_list(devices);
	if (reason != NULL)
		*reason = "no RDMA HCA reported by libibverbs";
	return false;
#else
	if (reason != NULL)
		*reason = "binary was not built with --with-rdma";
	return false;
#endif
}

#ifdef HAVE_LIBIBVERBS
static bool
rdma_mlx5_context_available(ClusterICRdmaCtx *ctx, const char **reason)
{
#ifdef HAVE_MLX5DV
	struct mlx5dv_context attrs;

	if (ctx == NULL || ctx->verbs == NULL) {
		if (reason != NULL)
			*reason = "mlx5dv probe requires an open RDMA device";
		return false;
	}

	memset(&attrs, 0, sizeof(attrs));
	if (mlx5dv_query_device(ctx->verbs, &attrs) == 0) {
		if (reason != NULL)
			*reason = NULL;
		return true;
	}
	if (reason != NULL)
		*reason = "mlx5dv_query_device failed for the selected RDMA device";
	return false;
#else
	return rdma_mlx5_build_available(reason);
#endif
}
#endif

const char *
cluster_ic_rdma_peer_state_name(ClusterICRdmaPeerState state)
{
	switch (state) {
	case CLUSTER_IC_RDMA_PEER_DISABLED:
		return "disabled";
	case CLUSTER_IC_RDMA_PEER_FALLBACK_TCP:
		return "fallback_tcp";
	case CLUSTER_IC_RDMA_PEER_CONNECTING:
		return "connecting";
	case CLUSTER_IC_RDMA_PEER_CONNECTED:
		return "connected";
	case CLUSTER_IC_RDMA_PEER_ERROR:
		return "error";
	}
	return "unknown";
}

void
cluster_ic_rdma_stats_note_transport(int32 peer_id, ClusterICPeerTransport transport,
									 ClusterICRdmaPeerState state)
{
	ClusterICRdmaPeerStats *p = rdma_peer_stats(peer_id);

	if (p == NULL)
		return;
	LWLockAcquire(&RdmaShmem->lock.lock, LW_EXCLUSIVE);
	p->transport = (int32)transport;
	p->state = (int32)state;
	strlcpy(p->provider, rdma_selected_provider_name(), sizeof(p->provider));
	LWLockRelease(&RdmaShmem->lock.lock);
}

void
cluster_ic_rdma_stats_note_fallback(int32 peer_id, const char *reason)
{
	ClusterICRdmaPeerStats *p;

	if (RdmaShmem != NULL)
		pg_atomic_fetch_add_u64(&RdmaShmem->global_fallback_count, 1);

	p = rdma_peer_stats(peer_id);
	if (p == NULL)
		return;

	LWLockAcquire(&RdmaShmem->lock.lock, LW_EXCLUSIVE);
	p->transport = CLUSTER_IC_PEER_TRANSPORT_TCP;
	p->state = CLUSTER_IC_RDMA_PEER_FALLBACK_TCP;
	pg_atomic_fetch_add_u64(&p->fallback_count, 1);
	strlcpy(p->provider, rdma_selected_provider_name(), sizeof(p->provider));
	if (reason != NULL)
		strlcpy(p->last_error, reason, sizeof(p->last_error));
	LWLockRelease(&RdmaShmem->lock.lock);
}

void
cluster_ic_rdma_stats_note_send(int32 peer_id, uint64 bytes, bool rdma)
{
	ClusterICRdmaPeerStats *p = rdma_peer_stats(peer_id);

	if (p == NULL)
		return;
	LWLockAcquire(&RdmaShmem->lock.lock, LW_EXCLUSIVE);
	p->transport = rdma ? CLUSTER_IC_PEER_TRANSPORT_RDMA : CLUSTER_IC_PEER_TRANSPORT_TCP;
	LWLockRelease(&RdmaShmem->lock.lock);
	pg_atomic_fetch_add_u64(&p->send_count, 1);
	pg_atomic_fetch_add_u64(&p->bytes_send, bytes);
	if (rdma)
		rdma_stats_note_tier3_send();
}

void
cluster_ic_rdma_stats_note_recv(int32 peer_id, uint64 bytes, bool rdma)
{
	ClusterICRdmaPeerStats *p = rdma_peer_stats(peer_id);

	if (p == NULL)
		return;
	LWLockAcquire(&RdmaShmem->lock.lock, LW_EXCLUSIVE);
	p->transport = rdma ? CLUSTER_IC_PEER_TRANSPORT_RDMA : CLUSTER_IC_PEER_TRANSPORT_TCP;
	LWLockRelease(&RdmaShmem->lock.lock);
	pg_atomic_fetch_add_u64(&p->recv_count, 1);
	pg_atomic_fetch_add_u64(&p->bytes_recv, bytes);
}

void
cluster_ic_rdma_stats_note_error(int32 peer_id, const char *sqlstate, const char *message)
{
	ClusterICRdmaPeerStats *p = rdma_peer_stats(peer_id);

	if (p == NULL)
		return;
	LWLockAcquire(&RdmaShmem->lock.lock, LW_EXCLUSIVE);
	p->state = CLUSTER_IC_RDMA_PEER_ERROR;
	if (sqlstate != NULL)
		strlcpy(p->last_error_code, sqlstate, sizeof(p->last_error_code));
	if (message != NULL)
		strlcpy(p->last_error, message, sizeof(p->last_error));
	LWLockRelease(&RdmaShmem->lock.lock);
}

void
cluster_ic_rdma_stats_note_cq_depth(int32 peer_id, int32 depth)
{
	ClusterICRdmaPeerStats *p = rdma_peer_stats(peer_id);

	if (p == NULL)
		return;
	LWLockAcquire(&RdmaShmem->lock.lock, LW_EXCLUSIVE);
	p->cq_depth = depth < 0 ? 0 : depth;
	LWLockRelease(&RdmaShmem->lock.lock);
}

void
cluster_ic_rdma_stats_note_mr_registered(bool registered)
{
	if (RdmaShmem == NULL)
		return;
	pg_atomic_write_u32(&RdmaShmem->mr_registered, registered ? 1 : 0);
}

bool
cluster_ic_rdma_block_sge_supported(const char **reason)
{
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	if (RdmaCtxOpen && RdmaProvider != NULL) {
		if (reason != NULL)
			*reason = NULL;
		return true;
	}
#endif
	if (reason != NULL)
		*reason = "RDMA SEND-with-SGE scratch path is not active; using envelope-safe TCP path";
	return false;
}

bool
cluster_ic_rdma_borrow_block_scratch(int32 peer_id, size_t len, void **out_addr, uint32 *out_lkey,
									 ClusterICSgeReleaseCallback *out_release_cb,
									 void **out_release_arg)
{
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	ClusterICRdmaPeer *peer;

	if (out_addr != NULL)
		*out_addr = NULL;
	if (out_lkey != NULL)
		*out_lkey = 0;
	if (out_release_cb != NULL)
		*out_release_cb = NULL;
	if (out_release_arg != NULL)
		*out_release_arg = NULL;

	if (!rdma_valid_peer_id(peer_id) || len == 0)
		return false;
	peer = &RdmaPeers[peer_id];
	if (!peer->connected || peer->block_scratch_buf == NULL || peer->block_scratch_mr == NULL
		|| len > peer->block_scratch_len || peer->block_scratch_borrowed)
		return false;

	peer->block_scratch_borrowed = true;
	if (out_addr != NULL)
		*out_addr = peer->block_scratch_buf;
	if (out_lkey != NULL)
		*out_lkey = peer->block_scratch_mr->lkey;
	if (out_release_cb != NULL)
		*out_release_cb = rdma_peer_release_block_scratch;
	if (out_release_arg != NULL)
		*out_release_arg = peer;
	return true;
#else
	return false;
#endif
}

bool
cluster_ic_rdma_pending_outbound(int32 peer_id)
{
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	ClusterICRdmaPeer *peer;

	if (!rdma_valid_peer_id(peer_id))
		return false;
	peer = &RdmaPeers[peer_id];
	return peer->send_busy || peer->queued_len > 0;
#else
	return false;
#endif
}

static uint32
rdma_compute_sge_crc(ClusterICEnvelope *env, const ClusterICSge *payload_sge, int n_sge)
{
	pg_crc32c crc;
	const uint8 *env_bytes = (const uint8 *)env;
	const size_t crc_offset = offsetof(ClusterICEnvelope, payload_crc32c);
	int i;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, env_bytes, crc_offset);
	for (i = 0; i < n_sge; i++) {
		if (payload_sge[i].len > 0 && payload_sge[i].addr != NULL)
			COMP_CRC32C(crc, payload_sge[i].addr, payload_sge[i].len);
	}
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static uint32
rdma_sum_sge_lengths(const ClusterICSge *payload_sge, int n_sge, bool *ok)
{
	uint64 total = 0;
	int i;

	if (ok != NULL)
		*ok = false;
	if (n_sge < 0 || n_sge > CLUSTER_IC_RDMA_MAX_SGE)
		return 0;
	if (n_sge > 0 && payload_sge == NULL)
		return 0;

	for (i = 0; i < n_sge; i++) {
		if (payload_sge[i].len > 0 && payload_sge[i].addr == NULL)
			return 0;
		total += payload_sge[i].len;
		if (total > PG_UINT32_MAX)
			return 0;
	}

	if (ok != NULL)
		*ok = true;
	return (uint32)total;
}

static void
rdma_release_sge_callbacks(const ClusterICSge *payload_sge, int n_sge)
{
	ClusterICSgeReleaseCallback release_cb[CLUSTER_IC_RDMA_MAX_SGE];
	void *release_arg[CLUSTER_IC_RDMA_MAX_SGE];
	int release_count = 0;
	int i;

	if (payload_sge == NULL || n_sge <= 0)
		return;

	for (i = 0; i < n_sge; i++) {
		bool duplicate = false;
		int j;

		if (payload_sge[i].release_cb == NULL)
			continue;
		for (j = 0; j < release_count; j++) {
			if (release_cb[j] == payload_sge[i].release_cb
				&& release_arg[j] == payload_sge[i].release_arg) {
				duplicate = true;
				break;
			}
		}
		if (duplicate)
			continue;
		if (release_count >= lengthof(release_cb))
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("too many RDMA SGE release callbacks")));
		release_cb[release_count] = payload_sge[i].release_cb;
		release_arg[release_count] = payload_sge[i].release_arg;
		release_count++;
	}

	for (i = 0; i < release_count; i++)
		release_cb[i](release_arg[i]);
}

static ClusterICSendResult
rdma_send_envelope_sge_fallback(const ClusterICEnvelope *env, int32 dest_node_id,
								const ClusterICSge *payload_sge, int n_sge, uint32 payload_len)
{
	char *frame = NULL;
	char *cursor;
	ClusterICSendResult rc;
	int i;

	if (RdmaShmem != NULL)
		pg_atomic_fetch_add_u64(&RdmaShmem->block_sge_fallback_count, 1);

	frame = (char *)palloc(sizeof(*env) + payload_len);
	memcpy(frame, env, sizeof(*env));
	cursor = frame + sizeof(*env);
	for (i = 0; i < n_sge; i++) {
		if (payload_sge[i].len == 0)
			continue;
		memcpy(cursor, payload_sge[i].addr, payload_sge[i].len);
		cursor += payload_sge[i].len;
	}

	/*
	 * Force the TCP fallback transport.  Calling cluster_ic_send_envelope()
	 * would re-enter the mux and route back to RDMA for a peer that is still
	 * marked RDMA-connected.
	 */
	pgstat_report_wait_start(WAIT_EVENT_INTERCONNECT_TCP_FALLBACK);
	rc = ClusterICOps_Tier1.send_bytes(dest_node_id, frame, sizeof(*env) + payload_len);
	pgstat_report_wait_end();
	if (rc == CLUSTER_IC_SEND_DONE)
		cluster_ic_rdma_stats_note_send(dest_node_id, sizeof(*env) + payload_len, false);
	else if (rc == CLUSTER_IC_SEND_HARD_ERROR)
		cluster_ic_rdma_stats_note_error(dest_node_id, "08R04", "TCP fallback send failed");
	rdma_release_sge_callbacks(payload_sge, n_sge);
	pfree(frame);
	return rc;
}

ClusterICSendResult
cluster_ic_rdma_send_envelope_sge(uint8 msg_type, int32 dest_node_id,
								  const ClusterICSge *payload_sge, int n_sge, uint32 payload_len)
{
	const ClusterICMsgTypeInfo *info;
	const char *reason = NULL;
	uint32 summed_len;
	bool sum_ok;
	ClusterICEnvelope env;

	if (msg_type == 0 || (int)msg_type >= CLUSTER_IC_MSG_TYPE_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cluster_ic_rdma_send_envelope_sge: msg_type %u out of range", msg_type)));

	info = cluster_ic_get_msg_type_info(msg_type);
	if (info == NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_ic msg_type %u not registered", msg_type),
						errhint("Each subsystem must call cluster_ic_register_msg_type "
								"in postmaster phase 1 (cluster_init_shmem).")));

	if ((info->allowed_producer_mask & (1u << MyBackendType)) == 0)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_ic msg_type %u (\"%s\") not allowed from BackendType %d",
							   msg_type, info->name, (int)MyBackendType)));

	if (dest_node_id == cluster_node_id) {
		rdma_release_sge_callbacks(payload_sge, n_sge);
		return CLUSTER_IC_SEND_DONE;
	}

	if ((uint32)dest_node_id == PGRAC_IC_BROADCAST && !info->broadcast_ok)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_ic msg_type %u (\"%s\") does not allow BROADCAST "
							   "destination",
							   msg_type, info->name)));

	if (payload_len > PGRAC_IC_PAYLOAD_MAX)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("cluster_ic envelope payload %u exceeds 16 MB limit", payload_len)));

	summed_len = rdma_sum_sge_lengths(payload_sge, n_sge, &sum_ok);
	if (!sum_ok || summed_len != payload_len)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cluster_ic_rdma_send_envelope_sge received invalid SGE payload"),
						errdetail("n_sge=%d, summed_len=%u, payload_len=%u", n_sge, summed_len,
								  payload_len)));

	if (!cluster_ic_envelope_build(&env, msg_type, (uint32)cluster_node_id, (uint32)dest_node_id,
								   NULL, 0))
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("cluster_ic_envelope_build failed for msg_type %u", msg_type)));
	env.payload_length = payload_len;
	env.payload_crc32c = 0;
	env.payload_crc32c = rdma_compute_sge_crc(&env, payload_sge, n_sge);

	if (!cluster_ic_rdma_block_sge_supported(&reason)
		|| cluster_ic_mux_peer_transport(dest_node_id) != CLUSTER_IC_PEER_TRANSPORT_RDMA) {
		cluster_ic_rdma_stats_note_fallback(dest_node_id,
											reason != NULL ? reason : "peer selected TCP fallback");
		return rdma_send_envelope_sge_fallback(&env, dest_node_id, payload_sge, n_sge, payload_len);
	}

#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	{
		ClusterICSendResult rdma_rc;

		rdma_rc = rdma_peer_post_sge(dest_node_id, &env, payload_sge, n_sge, payload_len);
		if (rdma_rc == CLUSTER_IC_SEND_DONE) {
			if (RdmaShmem != NULL)
				pg_atomic_fetch_add_u64(&RdmaShmem->block_sge_send_count, 1);
			return rdma_rc;
		}
		if (rdma_rc == CLUSTER_IC_SEND_WOULD_BLOCK)
			cluster_ic_rdma_stats_note_fallback(
				dest_node_id, "RDMA SEND-with-SGE backpressured; using TCP fallback");
		else
			cluster_ic_rdma_stats_note_fallback(dest_node_id,
												"RDMA SEND-with-SGE failed; using TCP fallback");
	}
#endif
	return rdma_send_envelope_sge_fallback(&env, dest_node_id, payload_sge, n_sge, payload_len);
}

#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
static bool
rdma_split_host_port(const char *addr, char *host, size_t host_len, char *port, size_t port_len)
{
	const char *colon;
	size_t hlen;

	if (addr == NULL || host == NULL || port == NULL || host_len == 0 || port_len == 0)
		return false;

	colon = strrchr(addr, ':');
	if (colon == NULL || colon == addr || *(colon + 1) == '\0')
		return false;
	hlen = colon - addr;
	if (hlen >= host_len || strlen(colon + 1) >= port_len)
		return false;
	memcpy(host, addr, hlen);
	host[hlen] = '\0';
	strlcpy(port, colon + 1, port_len);
	return true;
}

static void
rdma_cm_close(void)
{
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (RdmaPeers[i].id != NULL)
			rdma_peer_close(i, "RDMA CM shutdown", false);
	}
	if (RdmaListenId != NULL) {
		rdma_destroy_id(RdmaListenId);
		RdmaListenId = NULL;
	}
	if (RdmaCmChannel != NULL) {
		rdma_destroy_event_channel(RdmaCmChannel);
		RdmaCmChannel = NULL;
	}
}

static void
rdma_lmon_report_start_failure(const char *reason)
{
	RdmaUnavailableReason = reason;
	rdma_cm_close();
	if (RdmaProvider != NULL)
		RdmaProvider->device_close(&RdmaCtx);
	cluster_ic_rdma_stats_note_error(cluster_node_id, "58R16", reason);
	if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_OFF)
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_IC_RDMA_FABRIC_ERROR),
						errmsg("RDMA CM listener setup failed"),
						errdetail("%s", reason != NULL ? reason : "unknown RDMA CM failure")));

	ereport(LOG, (errmsg("RDMA CM listener setup failed; TCP fallback remains active"),
				  errdetail("%s", reason != NULL ? reason : "unknown RDMA CM failure")));
}
#endif

int
cluster_ic_rdma_lmon_cm_fd(void)
{
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	if (RdmaCmChannel != NULL)
		return RdmaCmChannel->fd;
#endif
	return -1;
}

int
cluster_ic_rdma_lmon_completion_fd(void)
{
#ifdef HAVE_LIBIBVERBS
	if (RdmaCtx.completion_channel != NULL)
		return RdmaCtx.completion_channel->fd;
#endif
	return -1;
}

void
cluster_ic_rdma_lmon_start(void)
{
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	const ClusterNodeInfo *self;
	const char *addr;
	char host[CLUSTER_NODE_ADDR_LEN];
	char port[16];
	struct addrinfo hints;
	struct addrinfo *res = NULL;
	int gai_rc;

	if (!IsUnderPostmaster || MyBackendType != B_LMON)
		ereport(FATAL, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("RDMA transport must be opened only by LMON")));

	if (RdmaProvider == NULL)
		RdmaProvider = rdma_select_provider();
	if (!RdmaCtxOpen && !RdmaProvider->device_open(&RdmaCtx)) {
		rdma_lmon_report_start_failure(RdmaUnavailableReason != NULL ? RdmaUnavailableReason
																	 : "RDMA device open failed");
		return;
	}
	if (RdmaProvider == &ClusterICRdmaProvider_Mlx5) {
		const char *mlx5_reason = NULL;

		if (!rdma_mlx5_context_available(&RdmaCtx, &mlx5_reason)) {
			if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_OFF)
				ereport(FATAL,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("tier3/mlx5 RDMA direct verbs are unavailable on this device"),
						 errdetail("%s",
								   mlx5_reason != NULL ? mlx5_reason : "mlx5dv probe failed")));
			ereport(LOG,
					(errmsg("tier3/mlx5 RDMA direct verbs unavailable on this device; "
							"using generic verbs"),
					 errdetail("%s",
							   mlx5_reason != NULL ? mlx5_reason : "mlx5dv probe failed")));
			RdmaProvider = &ClusterICRdmaProvider_Verbs;
		}
	}
	if (RdmaCtx.shared_mr == NULL
		&& !RdmaProvider->reg_region(&RdmaCtx, BufferBlocks, (size_t)NBuffers * (size_t)BLCKSZ,
									 &RdmaSharedBuffersMr)) {
		const char *reason = RdmaUnavailableReason != NULL
								 ? RdmaUnavailableReason
								 : "shared_buffers MR registration failed";

		cluster_ic_rdma_stats_note_error(cluster_node_id, "58R16", reason);
		rdma_cm_close();
		RdmaProvider->device_close(&RdmaCtx);
		ereport(FATAL, (errcode(ERRCODE_CLUSTER_IC_RDMA_FABRIC_ERROR),
						errmsg("RDMA shared_buffers memory registration failed"),
						errdetail("%s", reason)));
	}
	if (RdmaCmChannel != NULL)
		return;

	self = cluster_conf_lookup_node(cluster_node_id);
	if (self == NULL) {
		rdma_lmon_report_start_failure("local node is not declared in pgrac.conf");
		return;
	}
	addr = self->rdma_addr;
	if (!rdma_split_host_port(addr, host, sizeof(host), port, sizeof(port))) {
		rdma_lmon_report_start_failure("local RDMA address is missing or malformed");
		return;
	}

	RdmaCmChannel = rdma_create_event_channel();
	if (RdmaCmChannel == NULL) {
		rdma_lmon_report_start_failure("rdma_create_event_channel failed");
		return;
	}
	if (rdma_create_id(RdmaCmChannel, &RdmaListenId, NULL, RDMA_PS_TCP) != 0) {
		rdma_cm_close();
		rdma_lmon_report_start_failure("rdma_create_id(listener) failed");
		return;
	}

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;
	gai_rc = getaddrinfo(host, port, &hints, &res);
	if (gai_rc != 0 || res == NULL) {
		rdma_cm_close();
		rdma_lmon_report_start_failure("getaddrinfo(local RDMA address) failed");
		return;
	}

	if (rdma_bind_addr(RdmaListenId, res->ai_addr) != 0) {
		freeaddrinfo(res);
		rdma_cm_close();
		rdma_lmon_report_start_failure("rdma_bind_addr(listener) failed");
		return;
	}
	freeaddrinfo(res);

	if (rdma_listen(RdmaListenId, CLUSTER_MAX_NODES) != 0) {
		rdma_cm_close();
		rdma_lmon_report_start_failure("rdma_listen(listener) failed");
		return;
	}

	rdma_start_active_connects();
#endif
}

void
cluster_ic_rdma_lmon_stop(void)
{
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	rdma_cm_close();
	if (RdmaProvider != NULL)
		RdmaProvider->device_close(&RdmaCtx);
#endif
}

void
cluster_ic_rdma_lmon_handle_cm_events(void)
{
#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	struct rdma_cm_event *event = NULL;
	enum rdma_cm_event_type event_type;
	struct rdma_cm_id *id;
	struct rdma_conn_param conn_param;
	ClusterICRdmaPrivateData private_reply;
	const void *private_data = NULL;
	uint8 private_data_len = 0;
	int32 peer_id = -1;
	const char *reason = NULL;
	int status;

	if (RdmaCmChannel == NULL)
		return;
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_RDMA_CONNECT);
	if (rdma_get_cm_event(RdmaCmChannel, &event) != 0 || event == NULL) {
		pgstat_report_wait_end();
		return;
	}
	pgstat_report_wait_end();

	event_type = event->event;
	id = event->id;
	status = event->status;
	if (event_type == RDMA_CM_EVENT_CONNECT_REQUEST || event_type == RDMA_CM_EVENT_ESTABLISHED) {
		private_data = event->param.conn.private_data;
		private_data_len = event->param.conn.private_data_len;
	}
	peer_id = rdma_cm_id_peer(id);

	if (status != 0) {
		rdma_ack_cm_event(event);
		if (rdma_valid_peer_id(peer_id))
			rdma_peer_fail_or_fallback(peer_id, "RDMA CM event error");
		else
			cluster_ic_rdma_stats_note_error(cluster_node_id, "58R16", "RDMA CM event error");
		return;
	}

	switch (event_type) {
	case RDMA_CM_EVENT_CONNECT_REQUEST:
		if (!rdma_verify_private_hello(private_data, private_data_len, -1, &peer_id, &reason)) {
			rdma_reject(id, NULL, 0);
			cluster_ic_rdma_stats_note_error(cluster_node_id, "08P01",
											 reason != NULL ? reason : "RDMA HELLO rejected");
			rdma_ack_cm_event(event);
			rdma_destroy_id(id);
			return;
		}
		if (!rdma_prepare_peer_for_connect(peer_id, id, false)) {
			rdma_ack_cm_event(event);
			rdma_peer_fail_or_fallback(peer_id, RdmaUnavailableReason != NULL
													? RdmaUnavailableReason
													: "RDMA accept preparation failed");
			return;
		}
		if (!rdma_conn_params(&conn_param, &private_reply) || rdma_accept(id, &conn_param) != 0) {
			rdma_ack_cm_event(event);
			rdma_peer_fail_or_fallback(peer_id, "rdma_accept failed");
			return;
		}
		break;

	case RDMA_CM_EVENT_ADDR_RESOLVED:
		if (!rdma_valid_peer_id(peer_id)) {
			rdma_ack_cm_event(event);
			cluster_ic_rdma_stats_note_error(cluster_node_id, "58R16",
											 "RDMA address-resolved event lacked peer context");
			return;
		}
		if (!rdma_prepare_peer_for_connect(peer_id, id, true)) {
			rdma_ack_cm_event(event);
			rdma_peer_fail_or_fallback(peer_id, RdmaUnavailableReason != NULL
													? RdmaUnavailableReason
													: "RDMA active preparation failed");
			return;
		}
		if (rdma_resolve_route(id, cluster_interconnect_connect_timeout_ms) != 0) {
			rdma_ack_cm_event(event);
			rdma_peer_fail_or_fallback(peer_id, "rdma_resolve_route failed");
			return;
		}
		break;

	case RDMA_CM_EVENT_ROUTE_RESOLVED:
		if (!rdma_valid_peer_id(peer_id)) {
			rdma_ack_cm_event(event);
			cluster_ic_rdma_stats_note_error(cluster_node_id, "58R16",
											 "RDMA route-resolved event lacked peer context");
			return;
		}
		if (!rdma_conn_params(&conn_param, &private_reply) || rdma_connect(id, &conn_param) != 0) {
			rdma_ack_cm_event(event);
			rdma_peer_fail_or_fallback(peer_id, "rdma_connect failed");
			return;
		}
		break;

	case RDMA_CM_EVENT_ESTABLISHED: {
		int32 expected_peer = peer_id;
		int32 verified_peer = -1;

		if (!rdma_verify_private_hello(private_data, private_data_len, expected_peer,
									   &verified_peer, &reason)) {
			rdma_ack_cm_event(event);
			if (rdma_valid_peer_id(expected_peer))
				rdma_peer_fail_or_fallback(expected_peer,
										   reason != NULL ? reason : "RDMA HELLO verify failed");
			else
				cluster_ic_rdma_stats_note_error(
					cluster_node_id, "08P01", reason != NULL ? reason : "RDMA HELLO verify failed");
			return;
		}
		rdma_mark_peer_connected(verified_peer);
	} break;

	case RDMA_CM_EVENT_DISCONNECTED:
		rdma_ack_cm_event(event);
		if (rdma_valid_peer_id(peer_id))
			rdma_peer_fail_or_fallback(peer_id, "RDMA peer disconnected");
		return;

	case RDMA_CM_EVENT_REJECTED:
	case RDMA_CM_EVENT_ADDR_ERROR:
	case RDMA_CM_EVENT_ROUTE_ERROR:
	case RDMA_CM_EVENT_CONNECT_ERROR:
	case RDMA_CM_EVENT_UNREACHABLE:
		rdma_ack_cm_event(event);
		if (rdma_valid_peer_id(peer_id))
			rdma_peer_fail_or_fallback(peer_id, "RDMA CM connection event failed");
		else
			cluster_ic_rdma_stats_note_error(cluster_node_id, "58R16",
											 "RDMA CM connection event failed");
		return;

	case RDMA_CM_EVENT_TIMEWAIT_EXIT:
		break;

	default:
		break;
	}

	rdma_ack_cm_event(event);
#endif
}

#ifdef HAVE_LIBIBVERBS
static int
rdma_busypoll_drain(ClusterICWc *wc, int max)
{
	TimestampTz start;
	TimestampTz now;
	int n;
	int budget_us = cluster_interconnect_rdma_busypoll_us;
	bool budget_exhausted = false;

	if (budget_us <= 0)
		return rdma_provider_poll_cq(&RdmaCtx, wc, max);

	start = GetCurrentTimestamp();
	for (;;) {
		n = rdma_provider_poll_cq(&RdmaCtx, wc, max);
		if (n != 0)
			break;
		CHECK_FOR_INTERRUPTS();
		now = GetCurrentTimestamp();
		if (now - start >= (TimestampTz)budget_us) {
			budget_exhausted = true;
			break;
		}
	}
	now = GetCurrentTimestamp();
	rdma_stats_note_busypoll((uint64)((now > start) ? (now - start) : 0), budget_exhausted);
	return n;
}

static void
rdma_process_polled_completions(ClusterICWc *wc, int n)
{
	int i;

	if (wc == NULL || n <= 0)
		return;

	cluster_ic_rdma_stats_note_cq_depth(cluster_node_id, n);
	for (i = 0; i < n; i++) {
		int32 peer_id = rdma_wr_peer(wc[i].wc.wr_id);
		uint64 wr_type = wc[i].wc.wr_id & CLUSTER_IC_RDMA_WR_TYPE_MASK;

		if (!rdma_valid_peer_id(peer_id)) {
			cluster_ic_rdma_stats_note_error(cluster_node_id, "58R16",
											 "RDMA completion had invalid peer id");
			continue;
		}
		if (wc[i].status != IBV_WC_SUCCESS) {
			rdma_handle_completion_error(peer_id, wr_type, wc[i].status);
			continue;
		}
		if (wr_type == CLUSTER_IC_RDMA_WR_TYPE_SEND) {
#if defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
			rdma_peer_release_pending_send(&RdmaPeers[peer_id]);
			RdmaPeers[peer_id].send_busy = false;
			rdma_peer_flush_queued_send(peer_id);
#endif
		} else if (wr_type == CLUSTER_IC_RDMA_WR_TYPE_RECV) {
#if defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
			pgstat_report_wait_start(WAIT_EVENT_INTERCONNECT_RDMA_RECV);
			rdma_process_recv_completion(&RdmaPeers[peer_id], wc[i].wc.byte_len);
			pgstat_report_wait_end();
#endif
		}
	}
#if defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	rdma_dispatch_pending_frames();
#endif
}
#endif

void
cluster_ic_rdma_lmon_handle_completion_events(void)
{
#ifdef HAVE_LIBIBVERBS
	struct ibv_cq *cq = NULL;
	void *ctx = NULL;
	ClusterICWc wc[32];
	int n;

	if (RdmaCtx.cq == NULL)
		return;
	if ((ClusterICRdmaCompletionModel)cluster_interconnect_rdma_completion
		== CLUSTER_IC_RDMA_COMPLETION_BUSYPOLL) {
		pgstat_report_wait_start(WAIT_EVENT_INTERCONNECT_RDMA_BUSYPOLL);
		n = rdma_busypoll_drain(wc, lengthof(wc));
		pgstat_report_wait_end();
		rdma_process_polled_completions(wc, n);
		return;
	}

	if (RdmaCtx.completion_channel == NULL)
		return;
	pgstat_report_wait_start(WAIT_EVENT_CLUSTER_IC_RDMA_POLL);
	if (ibv_get_cq_event(RdmaCtx.completion_channel, &cq, &ctx) != 0 || cq == NULL) {
		pgstat_report_wait_end();
		return;
	}
	pgstat_report_wait_end();
	ibv_ack_cq_events(cq, 1);
	(void)ibv_req_notify_cq(cq, 0);

	n = rdma_provider_poll_cq(&RdmaCtx, wc, lengthof(wc));
	rdma_process_polled_completions(wc, n);
#endif
}

bool
cluster_ic_rdma_drain_recv(int32 *out_sender_node_id, void *buf, size_t bufsize,
						   size_t *out_received_len)
{
	return rdma_inbound_read(out_sender_node_id, buf, bufsize, out_received_len);
}

bool
cluster_ic_rdma_peek_sender(int32 *out_sender_node_id)
{
	if (RdmaInboundHead == NULL)
		return false;
	if (out_sender_node_id != NULL)
		*out_sender_node_id = RdmaInboundHead->peer_id;
	return true;
}

static bool
rdma_provider_device_open(ClusterICRdmaCtx *ctx)
{
#ifdef HAVE_LIBIBVERBS
	return rdma_ctx_open_first_device(ctx);
#else
	return cluster_ic_rdma_runtime_available(&RdmaUnavailableReason);
#endif
}

static bool
rdma_provider_reg_region(ClusterICRdmaCtx *ctx, void *base, size_t len, ClusterICMr *out)
{
#ifdef HAVE_LIBIBVERBS
	struct ibv_mr *mr;

	if (ctx == NULL || out == NULL || ctx->pd == NULL || base == NULL || len == 0) {
		RdmaUnavailableReason = "invalid RDMA MR registration request";
		return false;
	}

	if (base == BufferBlocks && len == (size_t)NBuffers * (size_t)BLCKSZ)
		return rdma_register_shared_buffers(ctx, out);

	mr = ibv_reg_mr(ctx->pd, base, len, IBV_ACCESS_LOCAL_WRITE);
	if (mr == NULL) {
		RdmaUnavailableReason = "ibv_reg_mr failed";
		return false;
	}
	out->base = base;
	out->len = len;
	out->lkey = mr->lkey;
	out->rkey = mr->rkey;
	return true;
#else
	RdmaUnavailableReason = "shared_buffers MR registration requires active RDMA data path";
	return false;
#endif
}

static bool
rdma_provider_qp_create(ClusterICRdmaCtx *ctx, int32 peer, ClusterICQp *out)
{
#ifdef HAVE_LIBIBVERBS
	struct ibv_qp_init_attr attr;

	if (ctx == NULL || out == NULL || ctx->pd == NULL || ctx->cq == NULL
		|| !rdma_valid_peer_id(peer)) {
		RdmaUnavailableReason = "invalid RDMA QP creation request";
		return false;
	}

	memset(&attr, 0, sizeof(attr));
	attr.send_cq = ctx->cq;
	attr.recv_cq = ctx->cq;
	attr.qp_type = IBV_QPT_RC;
	attr.cap.max_send_wr = cluster_interconnect_rdma_max_send_wr;
	attr.cap.max_recv_wr = cluster_interconnect_rdma_max_send_wr;
	attr.cap.max_send_sge = CLUSTER_IC_RDMA_MAX_SGE + 1;
	attr.cap.max_recv_sge = 1;
	attr.cap.max_inline_data = cluster_interconnect_rdma_inline_max;

	memset(out, 0, sizeof(*out));
	out->peer_id = peer;
	out->qp = ibv_create_qp(ctx->pd, &attr);
	if (out->qp == NULL) {
		RdmaUnavailableReason = "ibv_create_qp failed";
		return false;
	}
	return true;
#else
	RdmaUnavailableReason = "RC QP creation requires active RDMA data path";
	return false;
#endif
}

static ClusterICSendResult
rdma_provider_post_send(ClusterICQp *qp, const ClusterICSge *sge, int n_sge, bool signaled,
						bool inline_send, uint32 imm pg_attribute_unused())
{
#ifdef HAVE_LIBIBVERBS
	struct ibv_sge verbs_sge[CLUSTER_IC_RDMA_MAX_SGE + 1];
	struct ibv_send_wr wr;
	struct ibv_send_wr *bad_wr = NULL;
	int i;

	if (qp == NULL || qp->qp == NULL || sge == NULL || n_sge <= 0
		|| n_sge > CLUSTER_IC_RDMA_MAX_SGE + 1) {
		RdmaUnavailableReason = "invalid RDMA post_send request";
		return CLUSTER_IC_SEND_HARD_ERROR;
	}

	for (i = 0; i < n_sge; i++) {
		if (sge[i].addr == NULL || sge[i].len == 0 || sge[i].len > PG_UINT32_MAX) {
			RdmaUnavailableReason = "invalid RDMA SGE";
			return CLUSTER_IC_SEND_HARD_ERROR;
		}
		verbs_sge[i].addr = (uintptr_t)sge[i].addr;
		verbs_sge[i].length = (uint32)sge[i].len;
		verbs_sge[i].lkey = sge[i].lkey;
	}

	memset(&wr, 0, sizeof(wr));
	wr.wr_id = rdma_make_wr_id(CLUSTER_IC_RDMA_WR_TYPE_SEND, qp->peer_id);
	wr.opcode = IBV_WR_SEND;
	wr.sg_list = verbs_sge;
	wr.num_sge = n_sge;
	wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
	if (inline_send)
		wr.send_flags |= IBV_SEND_INLINE;
	if (ibv_post_send(qp->qp, &wr, &bad_wr) != 0) {
		RdmaUnavailableReason = "ibv_post_send failed";
		return CLUSTER_IC_SEND_HARD_ERROR;
	}
	return CLUSTER_IC_SEND_DONE;
#else
	RdmaUnavailableReason = "RDMA post_send requires active RDMA data path";
	return CLUSTER_IC_SEND_HARD_ERROR;
#endif
}

static bool
rdma_provider_post_recv(ClusterICQp *qp, ClusterICSge *sge)
{
#ifdef HAVE_LIBIBVERBS
	struct ibv_sge verbs_sge;
	struct ibv_recv_wr wr;
	struct ibv_recv_wr *bad_wr = NULL;

	if (qp == NULL || qp->qp == NULL || sge == NULL || sge->addr == NULL || sge->len == 0
		|| sge->len > PG_UINT32_MAX) {
		RdmaUnavailableReason = "invalid RDMA post_recv request";
		return false;
	}

	memset(&verbs_sge, 0, sizeof(verbs_sge));
	verbs_sge.addr = (uintptr_t)sge->addr;
	verbs_sge.length = (uint32)sge->len;
	verbs_sge.lkey = sge->lkey;
	memset(&wr, 0, sizeof(wr));
	wr.wr_id = rdma_make_wr_id(CLUSTER_IC_RDMA_WR_TYPE_RECV, qp->peer_id);
	wr.sg_list = &verbs_sge;
	wr.num_sge = 1;
	if (ibv_post_recv(qp->qp, &wr, &bad_wr) != 0) {
		RdmaUnavailableReason = "ibv_post_recv failed";
		return false;
	}
	return true;
#else
	RdmaUnavailableReason = "RDMA post_recv requires active RDMA data path";
	return false;
#endif
}

static int
rdma_provider_poll_cq(ClusterICRdmaCtx *ctx, ClusterICWc *out, int max)
{
#ifdef HAVE_LIBIBVERBS
	struct ibv_wc wc[32];
	int limit;
	int n;
	int i;

	if (ctx == NULL || ctx->cq == NULL || out == NULL || max <= 0)
		return 0;
	limit = Min(max, (int)lengthof(wc));
	n = ibv_poll_cq(ctx->cq, limit, wc);
	if (n <= 0)
		return n;
	for (i = 0; i < n; i++) {
		out[i].status = (int)wc[i].status;
		out[i].wc = wc[i];
	}
	return n;
#else
	return 0;
#endif
}

static void
rdma_provider_device_close(ClusterICRdmaCtx *ctx)
{
#ifdef HAVE_LIBIBVERBS
	rdma_ctx_close(ctx);
#endif
}

const ClusterICRdmaProvider ClusterICRdmaProvider_Verbs = {
	.name = "verbs-generic",
	.device_open = rdma_provider_device_open,
	.reg_region = rdma_provider_reg_region,
	.qp_create = rdma_provider_qp_create,
	.post_send = rdma_provider_post_send,
	.post_recv = rdma_provider_post_recv,
	.poll_cq = rdma_provider_poll_cq,
	.device_close = rdma_provider_device_close,
};

const ClusterICRdmaProvider ClusterICRdmaProvider_Mlx5 = {
	.name = "mlx5-direct",
	.device_open = rdma_provider_device_open,
	.reg_region = rdma_provider_reg_region,
	.qp_create = rdma_provider_qp_create,
	.post_send = rdma_provider_post_send,
	.post_recv = rdma_provider_post_recv,
	.poll_cq = rdma_provider_poll_cq,
	.device_close = rdma_provider_device_close,
};

static const ClusterICRdmaProvider *
rdma_select_provider(void)
{
	if (rdma_mlx5_requested() && rdma_mlx5_build_available(NULL))
		return &ClusterICRdmaProvider_Mlx5;
	return &ClusterICRdmaProvider_Verbs;
}

static void
rdma_tier_init(void)
{
	int i;
	const char *mlx5_reason = NULL;

	if (rdma_mlx5_requested() && !rdma_mlx5_build_available(&mlx5_reason)) {
		if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_OFF)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("tier3/mlx5 RDMA direct verbs are unavailable"),
					 errdetail("%s", mlx5_reason != NULL ? mlx5_reason : "mlx5dv is unavailable"),
					 errhint("Rebuild --with-rdma with libmlx5/mlx5dv support or set "
							 "cluster.interconnect_rdma_fallback=auto.")));
		ereport(LOG, (errmsg("tier3/mlx5 RDMA direct verbs unavailable; using generic verbs"),
					  errdetail("%s", mlx5_reason != NULL ? mlx5_reason : "mlx5dv is unavailable")));
	}

	if ((ClusterICRdmaCompletionModel)cluster_interconnect_rdma_completion
		== CLUSTER_IC_RDMA_COMPLETION_BUSYPOLL)
		ereport(LOG, (errmsg("RDMA busy-poll completion mode enabled"),
					  errdetail("cluster.interconnect_rdma_busypoll_us=%d",
								cluster_interconnect_rdma_busypoll_us)));

	if (cluster_interconnect_rdma_crc_offload)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("RDMA CRC offload is not implemented in spec-6.1"),
						errhint("Leave cluster.interconnect_rdma_crc_offload=off; "
								"block shipping always uses application CRC32C.")));

#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		memset(&RdmaPeers[i], 0, sizeof(RdmaPeers[i]));
		RdmaPeers[i].peer_id = i;
		RdmaPeers[i].qp.peer_id = i;
	}
#endif

	for (i = 0; i < CLUSTER_MAX_NODES; i++) {
		if (cluster_conf_lookup_node(i) == NULL)
			continue;
		if (i == cluster_node_id)
			continue;
		cluster_ic_rdma_stats_note_transport(i, CLUSTER_IC_PEER_TRANSPORT_TCP,
											 CLUSTER_IC_RDMA_PEER_DISABLED);
	}

	RdmaProvider = rdma_select_provider();
}

static void
rdma_tier_shutdown(void)
{
	if (RdmaProvider != NULL)
		RdmaProvider->device_close(&RdmaCtx);
	else
		rdma_select_provider()->device_close(&RdmaCtx);
}

static ClusterICSendResult
rdma_send_bytes(int32 target_node_id, const void *buf, size_t len)
{
	ClusterICSendResult rc;

#if defined(HAVE_LIBIBVERBS) && defined(HAVE_LIBRDMACM) && defined(HAVE_RDMA_RDMA_CMA_H)
	if (cluster_ic_mux_peer_transport(target_node_id) == CLUSTER_IC_PEER_TRANSPORT_RDMA) {
		rc = rdma_peer_send_bytes(target_node_id, buf, len);
		if (rc == CLUSTER_IC_SEND_DONE || rc == CLUSTER_IC_SEND_WOULD_BLOCK)
			return rc;
		if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_AUTO)
			cluster_ic_rdma_stats_note_fallback(target_node_id,
												"RDMA send failed; using TCP fallback");
	}
#endif

	if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_OFF) {
		cluster_ic_rdma_stats_note_error(target_node_id, "58R16", RdmaUnavailableReason);
		ereport(WARNING, (errcode(ERRCODE_CLUSTER_IC_RDMA_FABRIC_ERROR),
						  errmsg("RDMA send requested before RDMA data path is active"),
						  errdetail("%s", RdmaUnavailableReason)));
		return CLUSTER_IC_SEND_HARD_ERROR;
	}

	pgstat_report_wait_start(WAIT_EVENT_INTERCONNECT_TCP_FALLBACK);
	rc = ClusterICOps_Tier1.send_bytes(target_node_id, buf, len);
	pgstat_report_wait_end();
	if (rc == CLUSTER_IC_SEND_DONE)
		cluster_ic_rdma_stats_note_send(target_node_id, (uint64)len, false);
	else if (rc == CLUSTER_IC_SEND_HARD_ERROR)
		cluster_ic_rdma_stats_note_error(target_node_id, "08R04", "TCP fallback send failed");
	return rc;
}

static bool
rdma_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize, size_t *out_received_len)
{
	bool ok;

	ok = cluster_ic_rdma_drain_recv(out_sender_node_id, buf, bufsize, out_received_len);
	if (ok) {
		if (out_sender_node_id != NULL && out_received_len != NULL && *out_received_len > 0)
			cluster_ic_rdma_stats_note_recv(*out_sender_node_id, (uint64)*out_received_len, true);
		return true;
	}

	ok = ClusterICOps_Tier1.recv_bytes(out_sender_node_id, buf, bufsize, out_received_len);
	if (ok && out_sender_node_id != NULL && out_received_len != NULL && *out_received_len > 0)
		cluster_ic_rdma_stats_note_recv(*out_sender_node_id, (uint64)*out_received_len, false);
	return ok;
}

static bool
rdma_peek_sender(int32 *out_sender_node_id)
{
	if (cluster_ic_rdma_peek_sender(out_sender_node_id))
		return true;
	return ClusterICOps_Tier1.peek_sender(out_sender_node_id);
}

const ClusterICOps ClusterICOps_Tier2 = {
	.send_bytes = rdma_send_bytes,
	.recv_bytes = rdma_recv_bytes,
	.peek_sender = rdma_peek_sender,
	.tier_init = rdma_tier_init,
	.tier_shutdown = rdma_tier_shutdown,
	.tier_name = "tier2-rdma",
};

const ClusterICOps ClusterICOps_Tier3 = {
	.send_bytes = rdma_send_bytes,
	.recv_bytes = rdma_recv_bytes,
	.peek_sender = rdma_peek_sender,
	.tier_init = rdma_tier_init,
	.tier_shutdown = rdma_tier_shutdown,
	.tier_name = "tier3-rdma",
};

Datum
cluster_get_ic_rdma_peers(PG_FUNCTION_ARGS)
{
	InitMaterializedSRF(fcinfo, 0);

	{
		ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
		bool mr_registered = false;
		uint64 block_sge_send_count = 0;
		uint64 block_sge_fallback_count = 0;
		uint64 tier3_send_count = 0;
		uint64 inline_send_count = 0;
		uint64 unsignaled_batch_count = 0;
		uint64 busypoll_us_burned = 0;
		uint64 busypoll_fallback_count = 0;
		int i;

		if (ClusterConfShmem == NULL)
			return (Datum)0;

		if (RdmaShmem != NULL) {
			mr_registered = pg_atomic_read_u32(&RdmaShmem->mr_registered) != 0;
			block_sge_send_count = pg_atomic_read_u64(&RdmaShmem->block_sge_send_count);
			block_sge_fallback_count = pg_atomic_read_u64(&RdmaShmem->block_sge_fallback_count);
			tier3_send_count = pg_atomic_read_u64(&RdmaShmem->tier3_send_count);
			inline_send_count = pg_atomic_read_u64(&RdmaShmem->inline_send_count);
			unsignaled_batch_count = pg_atomic_read_u64(&RdmaShmem->unsignaled_batch_count);
			busypoll_us_burned = pg_atomic_read_u64(&RdmaShmem->busypoll_us_burned);
			busypoll_fallback_count = pg_atomic_read_u64(&RdmaShmem->busypoll_fallback_count);
		}

		for (i = 0; i < ClusterConfShmem->node_count; i++) {
			const ClusterNodeInfo *n = &ClusterConfShmem->nodes[i];
			ClusterICRdmaPeerStats *p = rdma_peer_stats(n->node_id);
			ClusterICPeerTransport transport = CLUSTER_IC_PEER_TRANSPORT_TCP;
			ClusterICRdmaPeerState state = CLUSTER_IC_RDMA_PEER_DISABLED;
			char provider_buf[CLUSTER_IC_RDMA_PROVIDER_NAME_LEN];
			char last_error_code_buf[CLUSTER_IC_RDMA_ERRCODE_LEN];
			char last_error_buf[CLUSTER_IC_RDMA_ERRMSG_LEN];
			int32 cq_depth = 0;
			Datum values[25];
			bool nulls[25] = { false };

			strlcpy(provider_buf, rdma_selected_provider_name(), sizeof(provider_buf));
			last_error_code_buf[0] = '\0';
			last_error_buf[0] = '\0';
			if (p != NULL) {
				LWLockAcquire(&RdmaShmem->lock.lock, LW_SHARED);
				transport = (ClusterICPeerTransport)p->transport;
				state = (ClusterICRdmaPeerState)p->state;
				cq_depth = p->cq_depth;
				if (p->provider[0] != '\0')
					strlcpy(provider_buf, p->provider, sizeof(provider_buf));
				if (p->last_error_code[0] != '\0')
					strlcpy(last_error_code_buf, p->last_error_code, sizeof(last_error_code_buf));
				if (p->last_error[0] != '\0')
					strlcpy(last_error_buf, p->last_error, sizeof(last_error_buf));
				LWLockRelease(&RdmaShmem->lock.lock);
			}

			values[0] = Int32GetDatum(n->node_id);
			values[1] = CStringGetTextDatum(cluster_ic_peer_transport_name(transport));
			values[2] = CStringGetTextDatum(cluster_ic_rdma_peer_state_name(state));
			values[3] = CStringGetTextDatum(provider_buf);

			if (n->rdma_addr[0] == '\0')
				nulls[4] = true;
			else
				values[4] = CStringGetTextDatum(n->rdma_addr);

			if (n->rdma_gid[0] == '\0')
				nulls[5] = true;
			else
				values[5] = CStringGetTextDatum(n->rdma_gid);

			values[6] = Int32GetDatum(n->rdma_port);
			values[7] = BoolGetDatum(mr_registered);
			values[8] = Int32GetDatum(cq_depth);
			values[9]
				= Int64GetDatum(p != NULL ? (int64)pg_atomic_read_u64(&p->fallback_count) : 0);
			values[10] = Int64GetDatum(p != NULL ? (int64)pg_atomic_read_u64(&p->send_count) : 0);
			values[11] = Int64GetDatum(p != NULL ? (int64)pg_atomic_read_u64(&p->recv_count) : 0);
			values[12] = Int64GetDatum(p != NULL ? (int64)pg_atomic_read_u64(&p->bytes_send) : 0);
			values[13] = Int64GetDatum(p != NULL ? (int64)pg_atomic_read_u64(&p->bytes_recv) : 0);
			values[14] = Int64GetDatum((int64)block_sge_send_count);
			values[15] = Int64GetDatum((int64)block_sge_fallback_count);
			values[16] = Int64GetDatum((int64)tier3_send_count);
			values[17] = Int64GetDatum((int64)inline_send_count);
			values[18] = Int64GetDatum((int64)unsignaled_batch_count);
			values[19] = Int64GetDatum((int64)busypoll_us_burned);
			values[20] = Int64GetDatum((int64)busypoll_fallback_count);
			values[21]
				= Int64GetDatum(p != NULL ? (int64)pg_atomic_read_u64(&p->latency_us_sum) : 0);
			values[22] = Int64GetDatum(
				p != NULL ? (int64)pg_atomic_read_u64(&p->latency_sample_count) : 0);

			if (last_error_code_buf[0] == '\0')
				nulls[23] = true;
			else
				values[23] = CStringGetTextDatum(last_error_code_buf);

			if (last_error_buf[0] == '\0')
				nulls[24] = true;
			else
				values[24] = CStringGetTextDatum(last_error_buf);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	return (Datum)0;
}
