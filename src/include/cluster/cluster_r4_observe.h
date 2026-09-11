/*-------------------------------------------------------------------------
 *
 * cluster_r4_observe.h
 *	Observation-only Stage 8 R4 event domain.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_R4_OBSERVE_H
#define CLUSTER_R4_OBSERVE_H

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_tx_resolve.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum ClusterR4Event {
	CLUSTER_R4_EVENT_CR_ROUTE_STARTED = 0,
	CLUSTER_R4_EVENT_CR_HOLDER_FULL = 1,
	CLUSTER_R4_EVENT_CR_HOLDER_RETRY = 2,
	CLUSTER_R4_EVENT_CR_HOLDER_FAIL_CLOSED = 3,
	CLUSTER_R4_EVENT_UNDO_FETCH_SERVED = 4,
	CLUSTER_R4_EVENT_UNDO_FETCH_DENIED = 5,
	CLUSTER_R4_EVENT_TX_UNKNOWN = 6,
	CLUSTER_R4_EVENT_TX_IN_PROGRESS = 7,
	CLUSTER_R4_EVENT_TX_PREPARED = 8,
	CLUSTER_R4_EVENT_TX_COMMITTED = 9,
	CLUSTER_R4_EVENT_TX_ABORTED = 10,
	CLUSTER_R4_EVENT_MULTI_SERVED = 11,
	CLUSTER_R4_EVENT_MULTI_UNKNOWN = 12,
	CLUSTER_R4_EVENT_SLOT_CAPACITY_RETRY = 13,
	CLUSTER_R4_EVENT_CR_REQUESTER_TERMINAL_RETRY = 14,
	CLUSTER_R4_EVENT_CR_REQUESTER_REPLY_PERIOD = 15,
	CLUSTER_R4_EVENT_CR_REQUESTER_BACKPRESSURE = 16
} ClusterR4Event;

typedef enum ClusterR4RefusalStage {
	CLUSTER_R4_REFUSAL_MASTER = 0,
	CLUSTER_R4_REFUSAL_HOLDER_ADMISSION = 1,
	CLUSTER_R4_REFUSAL_HOLDER_SHIP = 2
} ClusterR4RefusalStage;

#define CLUSTER_R4_REFUSAL_STAGE_COUNT 3
#define CLUSTER_R4_REFUSAL_REASON_COUNT (CLUSTER_CR_BUILD_PROTOCOL + 1)
#define CLUSTER_R4_REFUSAL_EVENT_BASE 17
#define CLUSTER_R4_REFUSAL_EVENT(stage, reason)                                                    \
	(CLUSTER_R4_REFUSAL_EVENT_BASE + (stage) * CLUSTER_R4_REFUSAL_REASON_COUNT + (reason))
#define CLUSTER_R4_OBSERVATION_EVENT_COUNT                                                         \
	(CLUSTER_R4_REFUSAL_EVENT_BASE                                                                 \
	 + CLUSTER_R4_REFUSAL_STAGE_COUNT * CLUSTER_R4_REFUSAL_REASON_COUNT)

extern void cluster_r4_observe(ClusterR4Event event, ClusterTxResolveReason tx_reason,
							  ClusterCrBuildReason cr_reason);
extern const char *cluster_r4_refusal_stage_name(ClusterR4RefusalStage stage);
extern void cluster_r4_observe_refusal(ClusterR4RefusalStage stage, ClusterCrBuildReason reason,
									   const BufferTag *tag, uint64 request_id, uint64 epoch,
									   int32 requester, int32 master, SCN read_scn);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_R4_OBSERVE_H */
