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

typedef enum ClusterICRdmaPeerState {
	CLUSTER_IC_RDMA_PEER_DISABLED = 0,
	CLUSTER_IC_RDMA_PEER_FALLBACK_TCP,
	CLUSTER_IC_RDMA_PEER_CONNECTING,
	CLUSTER_IC_RDMA_PEER_CONNECTED,
	CLUSTER_IC_RDMA_PEER_ERROR,
} ClusterICRdmaPeerState;

typedef struct ClusterICMr {
	void *base;
	size_t len;
	uint32 lkey;
	uint32 rkey;
} ClusterICMr;

typedef void (*ClusterICSgeReleaseCallback)(void *arg);

typedef struct ClusterICSge {
	void *addr;
	size_t len;
	uint32 lkey;
	ClusterICSgeReleaseCallback release_cb;
	void *release_arg;
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

extern void cluster_ic_rdma_shmem_register(void);
extern bool cluster_ic_rdma_runtime_available(const char **reason);
extern const char *cluster_ic_peer_transport_name(ClusterICPeerTransport transport);
extern const char *cluster_ic_rdma_peer_state_name(ClusterICRdmaPeerState state);
extern ClusterICPeerTransport cluster_ic_mux_peer_transport(int32 peer_id);
extern void cluster_ic_mux_set_peer_transport(int32 peer_id, ClusterICPeerTransport transport,
											  ClusterICRdmaPeerState state);
extern uint64 cluster_ic_mux_fallback_count(void);
extern void cluster_ic_rdma_stats_note_transport(int32 peer_id, ClusterICPeerTransport transport,
												 ClusterICRdmaPeerState state);
extern void cluster_ic_rdma_stats_note_fallback(int32 peer_id, const char *reason);
extern void cluster_ic_rdma_stats_note_send(int32 peer_id, uint64 bytes, bool rdma);
extern void cluster_ic_rdma_stats_note_recv(int32 peer_id, uint64 bytes, bool rdma);
extern void cluster_ic_rdma_stats_note_error(int32 peer_id, const char *sqlstate,
											 const char *message);
extern void cluster_ic_rdma_stats_note_cq_depth(int32 peer_id, int32 depth);
extern void cluster_ic_rdma_stats_note_mr_registered(bool registered);

extern int cluster_ic_rdma_lmon_cm_fd(void);
extern int cluster_ic_rdma_lmon_completion_fd(void);
extern void cluster_ic_rdma_lmon_start(void);
extern void cluster_ic_rdma_lmon_stop(void);
extern void cluster_ic_rdma_lmon_handle_cm_events(void);
extern void cluster_ic_rdma_lmon_handle_completion_events(void);
extern bool cluster_ic_rdma_drain_recv(int32 *out_sender_node_id, void *buf, size_t bufsize,
									   size_t *out_received_len);
extern bool cluster_ic_rdma_peek_sender(int32 *out_sender_node_id);

/*
 * D5 guard surface: callers may ask whether the current active transport can
 * carry a block image by RDMA SEND-with-SGE while preserving the envelope
 * verify/quarantine/install chain.  False means the caller must keep the
 * existing contiguous envelope path.
 */
extern bool cluster_ic_rdma_block_sge_supported(const char **reason);
extern ClusterICSendResult cluster_ic_rdma_send_envelope_sge(uint8 msg_type,
															 int32 dest_node_id,
															 const ClusterICSge *payload_sge,
															 int n_sge,
															 uint32 payload_len);

extern Datum cluster_get_ic_rdma_peers(PG_FUNCTION_ARGS);

#endif /* CLUSTER_IC_RDMA_H */
