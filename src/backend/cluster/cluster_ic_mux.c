/*-------------------------------------------------------------------------
 *
 * cluster_ic_mux.c
 *	  Per-peer TCP/RDMA transport mux for cluster_ic.
 *
 * Spec: spec-6.1-rdma-transport-stack.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic_rdma.h"
#include "cluster/cluster_ic_tier1.h"
#include "utils/elog.h"


static ClusterICPeerTransport MuxPeerTransport[CLUSTER_MAX_NODES];
static uint64 MuxFallbackCount = 0;


const char *
cluster_ic_peer_transport_name(ClusterICPeerTransport transport)
{
	switch (transport) {
	case CLUSTER_IC_PEER_TRANSPORT_TCP:
		return "tcp";
	case CLUSTER_IC_PEER_TRANSPORT_RDMA:
		return "rdma";
	}

	return "unknown";
}

ClusterICPeerTransport
cluster_ic_mux_peer_transport(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES)
		return CLUSTER_IC_PEER_TRANSPORT_TCP;

	return MuxPeerTransport[peer_id];
}

uint64
cluster_ic_mux_fallback_count(void)
{
	return MuxFallbackCount;
}

static ClusterICPeerTransport
mux_transport_for_peer(int32 peer_id)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES) {
		MuxFallbackCount++;
		return CLUSTER_IC_PEER_TRANSPORT_TCP;
	}

	return MuxPeerTransport[peer_id];
}

static void
mux_mark_all_tcp(void)
{
	int i;

	for (i = 0; i < CLUSTER_MAX_NODES; i++)
		MuxPeerTransport[i] = CLUSTER_IC_PEER_TRANSPORT_TCP;
}

static void
mux_tier_init(void)
{
	const char *reason = NULL;

	mux_mark_all_tcp();
	MuxFallbackCount = 0;
	if (!cluster_ic_rdma_runtime_available(&reason)) {

		if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_OFF)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_IC_RDMA_UNAVAILABLE),
					 errmsg("RDMA interconnect requested but unavailable"),
					 errdetail("%s", reason != NULL ? reason : "unknown RDMA availability failure"),
					 errhint("Use cluster.interconnect_rdma_fallback=auto for TCP fallback, or fix "
							 "the RDMA build/device configuration.")));

		MuxFallbackCount++;
		ereport(LOG,
				(errmsg("RDMA interconnect unavailable; using TCP fallback for all peers"),
				 errdetail("%s", reason != NULL ? reason : "unknown RDMA availability failure")));
	}

	/*
	 * Tier1 remains initialized even for tier2/tier3 so per-peer fallback can
	 * dispatch to TCP without reinitializing transport state after startup.
	 */
	ClusterICOps_Tier1.tier_init();
}

static void
mux_tier_shutdown(void)
{
	ClusterICOps_Tier1.tier_shutdown();
	mux_mark_all_tcp();
}

static ClusterICSendResult
mux_send_bytes(int32 target_node_id, const void *buf, size_t len)
{
	ClusterICPeerTransport transport;

	transport = mux_transport_for_peer(target_node_id);
	if (transport == CLUSTER_IC_PEER_TRANSPORT_RDMA) {
		if ((ClusterICTier)cluster_interconnect_tier == CLUSTER_IC_TIER_3)
			return ClusterICOps_Tier3.send_bytes(target_node_id, buf, len);
		return ClusterICOps_Tier2.send_bytes(target_node_id, buf, len);
	}

	return ClusterICOps_Tier1.send_bytes(target_node_id, buf, len);
}

static bool
mux_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize,
			   size_t *out_received_len)
{
	/*
	 * RDMA CQ drain plugs in here once a peer has an active QP.  Until then
	 * the mux drains the always-initialized TCP tier, preserving the public
	 * recv_bytes signature and the fallback correctness contract.
	 */
	return ClusterICOps_Tier1.recv_bytes(out_sender_node_id, buf, bufsize, out_received_len);
}

static bool
mux_peek_sender(int32 *out_sender_node_id)
{
	return ClusterICOps_Tier1.peek_sender(out_sender_node_id);
}

const ClusterICOps ClusterICOps_Mux = {
	.send_bytes = mux_send_bytes,
	.recv_bytes = mux_recv_bytes,
	.peek_sender = mux_peek_sender,
	.tier_init = mux_tier_init,
	.tier_shutdown = mux_tier_shutdown,
	.tier_name = "mux",
};
