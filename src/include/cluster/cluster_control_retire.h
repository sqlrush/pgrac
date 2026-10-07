/*-------------------------------------------------------------------------
 * cluster_control_retire.h -- LMON-owned ordered control retirement.
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONTROL_RETIRE_H
#define CLUSTER_CONTROL_RETIRE_H

#include "cluster/cluster_control_request.h"
#include "cluster/cluster_grd_work_queue.h"
#include "cluster/cluster_ic_envelope.h"

extern bool cluster_control_retire_is_frame(const void *payload, Size length);
extern bool cluster_control_retire_cut(const ClusterResId *resid, ClusterControlRequestCut *out);
extern void cluster_control_retire_lmon_start(void);
extern void cluster_control_retire_lmon_tick(void);
extern void cluster_control_retire_ingress(const ClusterICEnvelope *env, const void *payload);
extern void cluster_control_retire_drain(const ClusterGrdWorkItem *item);
extern bool cluster_control_retire_outbound_allowed(uint8 type, const void *payload, uint16 length);

/* Called only by the validated FIFO retirement consumer. */
extern ClusterControlRetireVerb
cluster_ges_control_retire_at_master(const ClusterControlRetireMessage *message,
									 const ClusterControlRequestCut *cut);

#endif
