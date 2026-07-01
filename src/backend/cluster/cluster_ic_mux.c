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

void
cluster_ic_mux_set_peer_transport(int32 peer_id, ClusterICPeerTransport transport,
								  ClusterICRdmaPeerState state)
{
	if (peer_id < 0 || peer_id >= CLUSTER_MAX_NODES) {
		MuxFallbackCount++;
		cluster_ic_rdma_stats_note_fallback(peer_id, "invalid peer id for RDMA mux update");
		return;
	}

	MuxPeerTransport[peer_id] = transport;
	cluster_ic_rdma_stats_note_transport(peer_id, transport, state);
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
		cluster_ic_rdma_stats_note_fallback(peer_id, "invalid peer id for RDMA mux");
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

	if ((ClusterICTier)cluster_interconnect_tier == CLUSTER_IC_TIER_3)
		ClusterICOps_Tier3.tier_init();
	else
		ClusterICOps_Tier2.tier_init();

	if (!cluster_ic_rdma_runtime_available(&reason)) {
		int i;

		if (cluster_interconnect_rdma_fallback == CLUSTER_IC_RDMA_FALLBACK_OFF)
			ereport(FATAL,
					(errcode(ERRCODE_CLUSTER_IC_RDMA_UNAVAILABLE),
					 errmsg("RDMA interconnect requested but unavailable"),
					 errdetail("%s", reason != NULL ? reason : "unknown RDMA availability failure"),
					 errhint("Use cluster.interconnect_rdma_fallback=auto for TCP fallback, or fix "
							 "the RDMA build/device configuration.")));

		MuxFallbackCount++;
		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (cluster_conf_lookup_node(i) == NULL || i == cluster_node_id)
				continue;
			cluster_ic_rdma_stats_note_transport(i, CLUSTER_IC_PEER_TRANSPORT_TCP,
												 CLUSTER_IC_RDMA_PEER_FALLBACK_TCP);
		}
	} else {
		int i;

		for (i = 0; i < CLUSTER_MAX_NODES; i++) {
			if (cluster_conf_lookup_node(i) == NULL || i == cluster_node_id)
				continue;
			cluster_ic_rdma_stats_note_transport(i, CLUSTER_IC_PEER_TRANSPORT_TCP,
												 CLUSTER_IC_RDMA_PEER_CONNECTING);
		}
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
	if ((ClusterICTier)cluster_interconnect_tier == CLUSTER_IC_TIER_3)
		ClusterICOps_Tier3.tier_shutdown();
	else
		ClusterICOps_Tier2.tier_shutdown();
	ClusterICOps_Tier1.tier_shutdown();
	mux_mark_all_tcp();
}

static ClusterICSendResult
mux_send_bytes(int32 target_node_id, const void *buf, size_t len)
{
	ClusterICPeerTransport transport;
	ClusterICSendResult rc;

	transport = mux_transport_for_peer(target_node_id);
	if (transport == CLUSTER_IC_PEER_TRANSPORT_RDMA) {
		if ((ClusterICTier)cluster_interconnect_tier == CLUSTER_IC_TIER_3)
			return ClusterICOps_Tier3.send_bytes(target_node_id, buf, len);
		return ClusterICOps_Tier2.send_bytes(target_node_id, buf, len);
	}

	rc = ClusterICOps_Tier1.send_bytes(target_node_id, buf, len);
	if (rc == CLUSTER_IC_SEND_DONE)
		cluster_ic_rdma_stats_note_send(target_node_id, (uint64)len, false);
	else if (rc == CLUSTER_IC_SEND_HARD_ERROR)
		cluster_ic_rdma_stats_note_error(target_node_id, "08R04", "TCP fallback send failed");
	return rc;
}

static bool
mux_recv_bytes(int32 *out_sender_node_id, void *buf, size_t bufsize,
			   size_t *out_received_len)
{
	if (!cluster_ic_rdma_drain_recv(out_sender_node_id, buf, bufsize, out_received_len))
		return false;
	if (out_received_len != NULL && *out_received_len > 0)
		return true;

	if (!ClusterICOps_Tier1.recv_bytes(out_sender_node_id, buf, bufsize, out_received_len))
		return false;
	if (out_sender_node_id != NULL && out_received_len != NULL && *out_received_len > 0)
		cluster_ic_rdma_stats_note_recv(*out_sender_node_id, (uint64)*out_received_len, false);
	return true;
}

static bool
mux_peek_sender(int32 *out_sender_node_id)
{
	if (cluster_ic_rdma_peek_sender(out_sender_node_id))
		return true;
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
