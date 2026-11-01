/*-------------------------------------------------------------------------
 * cluster_mx_history_graph.h
 *    Private, one-pass history worklist. No retention or member authority.
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MX_HISTORY_GRAPH_H
#define CLUSTER_MX_HISTORY_GRAPH_H
#include "c.h"
#if defined(USE_PGRAC_CLUSTER) && !defined(FRONTEND)
#include "cluster/cluster_mx_history.h"
#include "cluster/cluster_mx_history_record.h"
#include "utils/palloc.h"

typedef struct ClusterMxHistoryGraph ClusterMxHistoryGraph;
#define CMXH_NO_NODE SIZE_MAX
/* Views expire on graph cancellation. They are not local TT resolutions or
 * assembled member proofs. page_prior is the actual page/slot ordering edge. */
typedef struct ClusterMxHistoryNodeView {
	const ClusterMxHistoryRequest *request;
	const ClusterMxHistoryRecord *record;
	const ClusterMxHistoryPeer *peer;
	const char *bytes;
	size_t global_prev, page_prior;
} ClusterMxHistoryNodeView;
extern ClusterMxHistoryGraph *
cluster_mx_history_graph_create(const ClusterMxHistoryCapture *capture, MemoryContext context);
/* One bounded DFS step. PENDING+needs_read requests exactly this address;
 * PENDING without it is local progress. COMPLETE means graph traversal only. */
extern ClusterMxHistoryStep cluster_mx_history_graph_next(ClusterMxHistoryGraph *graph,
														  ClusterMxHistoryRequest *request,
														  bool *needs_read);
/* Only a successful original backend landing supplies this private result.
 * Failure poisons the entire graph; no partial node views are delivered. */
extern bool cluster_mx_history_graph_accept(ClusterMxHistoryGraph *graph,
											const ClusterMxHistoryPeerResult *result);
extern bool cluster_mx_history_graph_view(const ClusterMxHistoryGraph *graph, size_t ordinal,
										  ClusterMxHistoryNodeView *out);
extern size_t cluster_mx_history_graph_count(const ClusterMxHistoryGraph *graph);
extern void cluster_mx_history_graph_destroy(ClusterMxHistoryGraph *graph);
#endif
#endif
