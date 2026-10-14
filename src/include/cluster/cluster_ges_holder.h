/*-------------------------------------------------------------------------
 * cluster_ges_holder.h -- exact, read-only relation holder observations.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GES_HOLDER_H
#define CLUSTER_GES_HOLDER_H
#include "cluster/cluster_grd.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_ic_envelope.h"

/* Separate from replacement (18) and control retirement (19). */
#define CLUSTER_GES_HOLDER_QUERY_OPCODE 20
#define CLUSTER_GES_HOLDER_REPLY_OPCODE 21
#define CLUSTER_GES_HOLDER_PROBE_BYTES 80

typedef struct ClusterGesHolderProbe {
	ClusterResId resid;
	ClusterGrdHolderId holder;
	uint64 nonce;
	/* Collector-local route provenance, echoed unchanged by the master. */
	uint64 collector_generation;
	uint64 collector_boot;
	uint64 master_boot;
	bool reply;
	ClusterGrdHolderObservation observation;
	LOCKMODE mode;
} ClusterGesHolderProbe;

extern bool cluster_ges_holder_probe_is_frame(const void *payload, Size length);
extern bool cluster_ges_holder_probe_encode(const ClusterGesHolderProbe *probe, void *payload,
											Size length);
extern bool cluster_ges_holder_probe_decode(const void *payload, Size length,
											ClusterGesHolderProbe *probe);
extern bool cluster_ges_holder_probe_send(int32 master, const ClusterGesHolderProbe *probe);
extern void cluster_ges_holder_probe_ingress(const ClusterICEnvelope *env, const void *payload);
extern void cluster_ges_holder_probe_drain(const ClusterGrdWorkItem *item);

/* The collector must match its entire retained query, including the nonce.
 * An observation is not a terminal barrier or permission to free a fence. */
extern void cluster_ko_peer_holder_observed(int32 master, const ClusterGesHolderProbe *probe);
#endif
