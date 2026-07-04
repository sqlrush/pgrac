/*-------------------------------------------------------------------------
 *
 * cluster_ic_rdma.h
 *	  RDMA/Mux transport surface for the pgrac cluster interconnect.
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
 *	  src/include/cluster/cluster_ic_rdma.h
 *
 * NOTES
 *	  pgrac-original header.  Defines the spec-6.1 RDMA mux ABI,
 *	  provider vtable, SGE release contract, and observability surface used
 *	  by both backend code and standalone cluster_unit tests.
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

typedef enum ClusterICRdmaBlockReplyLaneState {
	CLUSTER_IC_RDMA_BLOCK_REPLY_DISABLED = 0,
	CLUSTER_IC_RDMA_BLOCK_REPLY_CONNECTING,
	CLUSTER_IC_RDMA_BLOCK_REPLY_CONNECTED,
	CLUSTER_IC_RDMA_BLOCK_REPLY_RESETTING,
	CLUSTER_IC_RDMA_BLOCK_REPLY_ERROR,
} ClusterICRdmaBlockReplyLaneState;

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

/*
 * spec-6.13 D3/D6 constants.  Keep these header-visible so cluster_unit can
 * pin the queue-depth policy and block-reply direct-land wire size without
 * linking the backend RDMA implementation.
 */
#define CLUSTER_IC_RDMA_SIGNAL_BATCH_CAP 32
#define CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES 84
#define CLUSTER_IC_RDMA_DIRECT_LAND_REPLY_BYTES \
	(CLUSTER_IC_RDMA_DIRECT_LAND_SIDECAR_BYTES + BLCKSZ)
#define CLUSTER_IC_RDMA_BLOCK_REPLY_MAX_RECV_SGE 2
#define CLUSTER_IC_RDMA_DIRECT_LAND_WR_TYPE_RECV UINT64CONST(0x5400000000000000)
#define CLUSTER_IC_RDMA_DIRECT_LAND_WR_TYPE_MASK UINT64CONST(0xFF00000000000000)
#define CLUSTER_IC_RDMA_DIRECT_LAND_WR_PEER_SHIFT 32
#define CLUSTER_IC_RDMA_DIRECT_LAND_WR_PEER_MASK UINT64CONST(0x00FFFFFF)
#define CLUSTER_IC_RDMA_DIRECT_LAND_WR_GENERATION_SHIFT 16
#define CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_MASK UINT64CONST(0xFFFF)
#define CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_VALUES \
	(CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_MASK + UINT64CONST(1))

static inline uint32
cluster_ic_rdma_signal_batch_k(uint32 max_send_wr)
{
	uint32 k = max_send_wr / 2;

	if (k == 0)
		k = 1;
	if (k > CLUSTER_IC_RDMA_SIGNAL_BATCH_CAP)
		k = CLUSTER_IC_RDMA_SIGNAL_BATCH_CAP;
	return k;
}

static inline bool
cluster_ic_rdma_payload_inline_eligible(uint32 payload_len, int inline_max)
{
	return inline_max > 0 && payload_len <= (uint32)inline_max;
}

static inline bool
cluster_ic_rdma_direct_land_wr_field_valid(uint32 value)
{
	return value <= (uint32)CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_MASK;
}

static inline bool
cluster_ic_rdma_direct_land_arm_capacity_valid(uint32 capacity)
{
	return capacity <= (uint32)CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_VALUES;
}

static inline uint32
cluster_ic_rdma_direct_land_next_generation(uint32 generation)
{
	generation = (generation + 1) & (uint32)CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_MASK;
	return generation == 0 ? 1 : generation;
}

static inline uint64
cluster_ic_rdma_direct_land_make_wr_id(uint32 peer_id, uint32 arm_id, uint32 generation)
{
	return CLUSTER_IC_RDMA_DIRECT_LAND_WR_TYPE_RECV
		   | (((uint64)peer_id & CLUSTER_IC_RDMA_DIRECT_LAND_WR_PEER_MASK)
			  << CLUSTER_IC_RDMA_DIRECT_LAND_WR_PEER_SHIFT)
		   | (((uint64)generation & CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_MASK)
			  << CLUSTER_IC_RDMA_DIRECT_LAND_WR_GENERATION_SHIFT)
		   | ((uint64)arm_id & CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_MASK);
}

static inline bool
cluster_ic_rdma_direct_land_decode_wr_id(uint64 wr_id, uint32 *peer_id, uint32 *arm_id,
										 uint32 *generation)
{
	if ((wr_id & CLUSTER_IC_RDMA_DIRECT_LAND_WR_TYPE_MASK)
		!= CLUSTER_IC_RDMA_DIRECT_LAND_WR_TYPE_RECV)
		return false;
	if (peer_id != NULL)
		*peer_id = (uint32)((wr_id >> CLUSTER_IC_RDMA_DIRECT_LAND_WR_PEER_SHIFT)
							& CLUSTER_IC_RDMA_DIRECT_LAND_WR_PEER_MASK);
	if (generation != NULL)
		*generation = (uint32)((wr_id >> CLUSTER_IC_RDMA_DIRECT_LAND_WR_GENERATION_SHIFT)
							   & CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_MASK);
	if (arm_id != NULL)
		*arm_id = (uint32)(wr_id & CLUSTER_IC_RDMA_DIRECT_LAND_WR_FIELD_MASK);
	return true;
}

typedef struct ClusterICRdmaCtx ClusterICRdmaCtx;
typedef struct ClusterICQp ClusterICQp;
typedef struct ClusterICWc ClusterICWc;

typedef struct ClusterICRdmaProvider {
	const char *name;
	bool (*device_open)(ClusterICRdmaCtx *ctx);
	bool (*reg_region)(ClusterICRdmaCtx *ctx, void *base, size_t len, ClusterICMr *out);
	bool (*qp_create)(ClusterICRdmaCtx *ctx, int32 peer, ClusterICQp *out);
	ClusterICSendResult (*post_send)(ClusterICQp *qp, const ClusterICSge *sge, int n_sge,
									 bool signaled, bool inline_send, uint32 imm);
	bool (*post_recv)(ClusterICQp *qp, const ClusterICSge *sge, int n_sge, uint64 wr_id);
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
extern bool cluster_ic_mux_peer_has_pending_outbound(int32 peer_id);
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
extern bool cluster_ic_rdma_pending_outbound(int32 peer_id);

/*
 * D5 guard surface: callers may ask whether the current active transport can
 * carry a block image by RDMA SEND-with-SGE while preserving the envelope
 * verify/quarantine/install chain.  A block payload may be either a
 * registered per-peer scratch MR or a raw-pinned shared_buffers page whose
 * address is validated against the shared_buffers MR by
 * cluster_ic_rdma_shared_buffers_sge().  The GCS owner remains responsible
 * for WAL flush/revalidation and release after SEND completion/fallback.
 * False means the caller must keep the existing contiguous envelope path.
 * This is sender-side SGE support only, not receiver direct-land.
 */
extern bool cluster_ic_rdma_block_sge_supported(const char **reason);
extern bool cluster_ic_rdma_block_reply_lane_connected(int32 peer_id, const char **reason);
extern bool cluster_ic_rdma_shared_buffers_sge(void *addr, size_t len, uint32 *out_lkey);
extern bool cluster_ic_rdma_borrow_block_scratch(int32 peer_id, size_t len, void **out_addr,
												 uint32 *out_lkey,
												 ClusterICSgeReleaseCallback *out_release_cb,
												 void **out_release_arg);
extern ClusterICSendResult cluster_ic_rdma_send_envelope_sge(uint8 msg_type, int32 dest_node_id,
															 const ClusterICSge *payload_sge,
															 int n_sge, uint32 payload_len);
extern bool cluster_ic_rdma_block_reply_post_recv(int32 peer_id, uint32 arm_id,
												  uint32 generation, void *target_addr,
												  uint32 target_lkey);
extern ClusterICSendResult cluster_ic_rdma_send_block_reply_direct(
	int32 dest_node_id, const ClusterICSge *payload_sge, int n_sge, uint32 payload_len);
extern void cluster_ic_rdma_block_reply_abort_peer(int32 peer_id, const char *reason);

extern Datum cluster_get_ic_rdma_peers(PG_FUNCTION_ARGS);

#endif /* CLUSTER_IC_RDMA_H */
