/*-------------------------------------------------------------------------
 *
 * cluster_ic_rdma.c
 *	  RDMA provider scaffolding and tier2/tier3 vtables.
 *
 * Spec: spec-6.1-rdma-transport-stack.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_tier1.h"
#include "utils/elog.h"

#ifdef HAVE_LIBIBVERBS
#include <infiniband/verbs.h>
#endif


struct ClusterICRdmaCtx {
	int unused;
};

struct ClusterICQp {
	int32 peer_id;
};

struct ClusterICWc {
	int status;
};

static const char *RdmaUnavailableReason = "RDMA data path has not been initialized";


bool
cluster_ic_rdma_runtime_available(const char **reason)
{
#ifdef HAVE_LIBIBVERBS
	struct ibv_device **devices;
	int ndev = 0;

	devices = ibv_get_device_list(&ndev);
	if (devices != NULL && ndev > 0) {
		ibv_free_device_list(devices);
		if (reason != NULL)
			*reason = "RDMA HCA detected, but the RDMA data path is not active";
		return false;
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

bool
cluster_ic_rdma_block_sge_supported(const char **reason)
{
	if (reason != NULL)
		*reason = "RDMA SEND-with-SGE block path is not active; using envelope-safe TCP path";
	return false;
}

static bool
rdma_provider_device_open(ClusterICRdmaCtx *ctx pg_attribute_unused())
{
	return cluster_ic_rdma_runtime_available(&RdmaUnavailableReason);
}

static bool
rdma_provider_reg_region(ClusterICRdmaCtx *ctx pg_attribute_unused(),
						 void *base pg_attribute_unused(),
						 size_t len pg_attribute_unused(),
						 ClusterICMr *out pg_attribute_unused())
{
	RdmaUnavailableReason = "shared_buffers MR registration requires active RDMA data path";
	return false;
}

static bool
rdma_provider_qp_create(ClusterICRdmaCtx *ctx pg_attribute_unused(),
						int32 peer pg_attribute_unused(),
						ClusterICQp *out pg_attribute_unused())
{
	RdmaUnavailableReason = "RC QP creation requires active RDMA data path";
	return false;
}

static ClusterICSendResult
rdma_provider_post_send(ClusterICQp *qp pg_attribute_unused(),
						const ClusterICSge *sge pg_attribute_unused(),
						int n_sge pg_attribute_unused(),
						bool signaled pg_attribute_unused(),
						uint32 imm pg_attribute_unused())
{
	RdmaUnavailableReason = "RDMA post_send requires active RDMA data path";
	return CLUSTER_IC_SEND_HARD_ERROR;
}

static bool
rdma_provider_post_recv(ClusterICQp *qp pg_attribute_unused(),
						ClusterICSge *sge pg_attribute_unused())
{
	RdmaUnavailableReason = "RDMA post_recv requires active RDMA data path";
	return false;
}

static int
rdma_provider_poll_cq(ClusterICRdmaCtx *ctx pg_attribute_unused(),
					  ClusterICWc *out pg_attribute_unused(),
					  int max pg_attribute_unused())
{
	return 0;
}

static void
rdma_provider_device_close(ClusterICRdmaCtx *ctx pg_attribute_unused())
{
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

static void
rdma_tier_init(void)
{
	const char *reason = NULL;

	if (!cluster_ic_rdma_runtime_available(&reason)) {
		if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_OFF)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_IC_RDMA_UNAVAILABLE),
					 errmsg("RDMA interconnect requested but unavailable"),
					 errdetail("%s", reason != NULL ? reason : "unknown RDMA availability failure"),
					 errhint("Build with --with-rdma and configure an RDMA-capable device, or set "
							 "cluster.interconnect_rdma_fallback=auto.")));

		ereport(LOG,
				(errmsg("RDMA interconnect unavailable; falling back to TCP"),
				 errdetail("%s", reason != NULL ? reason : "unknown RDMA availability failure")));
	}
}

static void
rdma_tier_shutdown(void)
{
}

static ClusterICSendResult
rdma_send_bytes(int32 target_node_id, const void *buf, size_t len)
{
	if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_OFF) {
		ereport(WARNING,
				(errcode(ERRCODE_CLUSTER_IC_RDMA_FABRIC_ERROR),
				 errmsg("RDMA send requested before RDMA data path is active"),
				 errdetail("%s", RdmaUnavailableReason)));
		return CLUSTER_IC_SEND_HARD_ERROR;
	}

	return ClusterICOps_Tier1.send_bytes(target_node_id, buf, len);
}

static bool
rdma_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize,
				size_t *out_received_len)
{
	return ClusterICOps_Tier1.recv_bytes(out_sender_node_id, buf, bufsize, out_received_len);
}

static bool
rdma_peek_sender(int32 *out_sender_node_id)
{
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
