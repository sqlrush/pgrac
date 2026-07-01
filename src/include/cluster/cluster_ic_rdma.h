/*-------------------------------------------------------------------------
 *
 * cluster_ic_rdma.h
 *	  RDMA/Mux transport surface for the pgrac cluster interconnect.
 *
 * Spec: spec-6.1-rdma-transport-stack.md
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_IC_RDMA_H
#define CLUSTER_IC_RDMA_H

#include "cluster/cluster_ic.h"

/*
 * GUC enums.  Keep the C values stable because pg_settings stores the
 * integer selected by cluster_guc.c's config_enum_entry tables.
 */
typedef enum ClusterICRdmaFallbackPolicy {
	CLUSTER_IC_RDMA_FALLBACK_AUTO = 0,
	CLUSTER_IC_RDMA_FALLBACK_OFF,
} ClusterICRdmaFallbackPolicy;

typedef enum ClusterICRdmaProviderId {
	CLUSTER_IC_RDMA_PROVIDER_AUTO = 0,
	CLUSTER_IC_RDMA_PROVIDER_VERBS,
	CLUSTER_IC_RDMA_PROVIDER_MLX5,
} ClusterICRdmaProviderId;

typedef enum ClusterICRdmaCompletionModel {
	CLUSTER_IC_RDMA_COMPLETION_EVENT = 0,
	CLUSTER_IC_RDMA_COMPLETION_BUSYPOLL,
} ClusterICRdmaCompletionModel;

typedef enum ClusterICPeerTransport {
	CLUSTER_IC_PEER_TRANSPORT_TCP = 0,
	CLUSTER_IC_PEER_TRANSPORT_RDMA,
} ClusterICPeerTransport;

typedef struct ClusterICMr {
	void *base;
	size_t len;
	uint32 lkey;
	uint32 rkey;
} ClusterICMr;

typedef struct ClusterICSge {
	void *addr;
	size_t len;
	uint32 lkey;
} ClusterICSge;

typedef struct ClusterICRdmaCtx ClusterICRdmaCtx;
typedef struct ClusterICQp ClusterICQp;
typedef struct ClusterICWc ClusterICWc;

typedef struct ClusterICRdmaProvider {
	const char *name;
	bool (*device_open)(ClusterICRdmaCtx *ctx);
	bool (*reg_region)(ClusterICRdmaCtx *ctx, void *base, size_t len, ClusterICMr *out);
	bool (*qp_create)(ClusterICRdmaCtx *ctx, int32 peer, ClusterICQp *out);
	ClusterICSendResult (*post_send)(ClusterICQp *qp, const ClusterICSge *sge,
									 int n_sge, bool signaled, uint32 imm);
	bool (*post_recv)(ClusterICQp *qp, ClusterICSge *sge);
	int (*poll_cq)(ClusterICRdmaCtx *ctx, ClusterICWc *out, int max);
	void (*device_close)(ClusterICRdmaCtx *ctx);
} ClusterICRdmaProvider;

extern const ClusterICOps ClusterICOps_Mux;
extern const ClusterICOps ClusterICOps_Tier2;
extern const ClusterICOps ClusterICOps_Tier3;

extern const ClusterICRdmaProvider ClusterICRdmaProvider_Verbs;
extern const ClusterICRdmaProvider ClusterICRdmaProvider_Mlx5;

extern int cluster_interconnect_rdma_fallback;
extern int cluster_interconnect_rdma_provider;
extern int cluster_interconnect_rdma_completion;
extern int cluster_interconnect_rdma_busypoll_us;
extern bool cluster_interconnect_rdma_crc_offload;
extern int cluster_interconnect_rdma_inline_max;
extern int cluster_interconnect_rdma_max_send_wr;

extern bool cluster_ic_rdma_runtime_available(const char **reason);
extern const char *cluster_ic_peer_transport_name(ClusterICPeerTransport transport);
extern ClusterICPeerTransport cluster_ic_mux_peer_transport(int32 peer_id);
extern uint64 cluster_ic_mux_fallback_count(void);

/*
 * D5 guard surface: callers may ask whether the current active transport can
 * carry a block image by RDMA SEND-with-SGE while preserving the envelope
 * verify/quarantine/install chain.  False means the caller must keep the
 * existing contiguous envelope path.
 */
extern bool cluster_ic_rdma_block_sge_supported(const char **reason);

#endif /* CLUSTER_IC_RDMA_H */
