/* Read-only native diagnostics. Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_WAIT_GRAPH_H
#define CLUSTER_WAIT_GRAPH_H

#include "cluster/cluster_grd.h"
#include "cluster/cluster_wait_trace.h"

#ifdef USE_PGRAC_CLUSTER
typedef enum ClusterWaitGraphStatus {
	CLWG_OBSERVED, CLWG_BUSY, CLWG_ABSENT, CLWG_UNAVAILABLE, CLWG_INVALID
} ClusterWaitGraphStatus;

#define CLWG_PCM_QUEUE 64
#define CLWG_KO_ROWS 192

typedef struct ClusterWaitGraphPcmSnapshot {
	ClusterWaitGraphStatus status;
	ClusterWaitTrace view;
	uint32 nqueued;
	ClusterWaitTrace queue[CLWG_PCM_QUEUE];
} ClusterWaitGraphPcmSnapshot;

typedef struct ClusterWaitGraphKoRow {
	uint32 kind, slot;
	int32 pid, executor;
	uint64 serial, epoch;
	RelFileLocator locator;
	int32 fork;
	bool failed, raw_copy, buffer_copy, complete, observed, reserved, invalid;
	uint32 cursor, first, count;
	uint64 lsn;
	uint32 sent, done, uncertain, failed_peers;
} ClusterWaitGraphKoRow;

typedef struct ClusterWaitGraphKoSnapshot {
	ClusterWaitGraphStatus status;
	uint32 nrows, omitted;
	ClusterWaitGraphKoRow rows[CLWG_KO_ROWS];
} ClusterWaitGraphKoSnapshot;

extern void cluster_pcm_wait_graph_snapshot(const BufferTag *tag,
	ClusterWaitGraphPcmSnapshot *out);
extern void cluster_ko_wait_graph_snapshot(ClusterWaitGraphKoSnapshot *out);
#endif
#endif
