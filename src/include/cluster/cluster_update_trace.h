/*-------------------------------------------------------------------------
 * cluster_update_trace.h
 *   Opt-in whole-UPDATE execution trace; no authority or wait decisions.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *   src/include/cluster/cluster_update_trace.h
 * NOTES
 *   PGRAC-original diagnostic surface. A finite capture never overwrites an
 *   earlier operation. Restart a diagnostic instance for a new capture.
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UPDATE_TRACE_H
#define CLUSTER_UPDATE_TRACE_H
#ifndef FRONTEND

#include "cluster/cluster_xnode_profile.h"

#define CLUSTER_UPDATE_TRACE_MAX_RECORDS 32768
#define CLUSTER_UPDATE_TRACE_MAX_EVENTS 524288
#define CLUTRACE_STATUS_OK 0U
#define CLUTRACE_STATUS_ERROR 1U
#define CLUTRACE_STATUS_INCOMPLETE 2U

typedef struct ClusterUpdateTraceSnapshot {
	uint64 op_id;
	uint64 backend_sequence;
	int32 node_id;
	int32 backend_id;
	int32 pid;
	uint32 status;
	uint64 start_ns;
	uint64 end_ns;
	uint64 affected_rows;
	uint64 unattributed_ns;
	uint32 accounting_errors;
	uint64 phase_nanos[CLXP_NBUCKETS];
	uint64 phase_exclusive_nanos[CLXP_NBUCKETS];
	uint32 phase_events[CLXP_NBUCKETS];
} ClusterUpdateTraceSnapshot;

typedef struct ClusterUpdateTraceRecord {
	/* 0 = unpublished, 1 = immutable identity available, 2 = complete. */
	pg_atomic_uint32 publication;
	ClusterUpdateTraceSnapshot data;
} ClusterUpdateTraceRecord;

typedef enum ClusterUpdateTraceEventKind {
	CLUTRACE_CR_LINK = 1,
	CLUTRACE_CR_STATE,
	CLUTRACE_CR_BUILD_BEGIN,
	CLUTRACE_CR_BUILD_END,
	CLUTRACE_CR_RELEASE_ATTEMPT,
	CLUTRACE_CR_ORIGIN_GATE,
	CLUTRACE_LEGACY_SERVE_BEGIN,
	CLUTRACE_LEGACY_SERVE_END,
	CLUTRACE_ORIGIN_PHASE,
	CLUTRACE_CR_DEPENDENCY,
	CLUTRACE_ORIGIN_INCOMPLETE
} ClusterUpdateTraceEventKind;

/* Existing request identity only: no wire or authority additions. */
typedef struct ClusterUpdateTraceEvent {
	uint64 stamp_ns;
	uint64 request_id;
	uint64 op_id;
	uint64 generation;
	uint64 duration_ns;
	uint64 work_ns;
	uint64 dependency_request_id;
	int32 node_id;
	int32 pid;
	int32 requester_node;
	int32 requester_backend;
	uint32 slot;
	uint32 kind;
	uint32 value;
} ClusterUpdateTraceEvent;

/* Process-local asynchronous phase residence, separate from step-call work. */
typedef struct ClusterUpdateTraceInterval {
	ClusterUpdateTraceEvent event;
	uint64 start_ns;
	uint64 entry_ns;
	uint64 work_ns;
	bool active;
} ClusterUpdateTraceInterval;

typedef struct ClusterUpdateTraceEventRecord {
	pg_atomic_uint32 published;
	ClusterUpdateTraceEvent data;
} ClusterUpdateTraceEventRecord;

typedef struct ClusterUpdateTraceShared {
	pg_atomic_uint64 next_op_id;
	ClusterUpdateTraceRecord records[CLUSTER_UPDATE_TRACE_MAX_RECORDS];
	pg_atomic_uint64 next_event;
	ClusterUpdateTraceEventRecord events[CLUSTER_UPDATE_TRACE_MAX_EVENTS];
} ClusterUpdateTraceShared;

/* Initialized before PG_TRY; mutable accounting is backend-local. */
typedef struct ClusterUpdateTraceScope {
	bool active;
	uint64 op_id;
} ClusterUpdateTraceScope;

extern PGDLLIMPORT bool cluster_update_trace_enabled;
extern PGDLLIMPORT ClusterUpdateTraceShared *ClusterUpdateTraceCtl;
extern Size cluster_update_trace_shmem_size(void);
extern void cluster_update_trace_shmem_init(void);
extern void cluster_update_trace_shmem_register(void);
extern void cluster_update_trace_begin_at(ClusterUpdateTraceScope *scope, uint64 now);
extern void cluster_update_trace_end_at(ClusterUpdateTraceScope *scope, uint32 status,
										uint64 affected_rows, uint64 now);
extern uint32 cluster_update_trace_snapshot_count(void);
extern uint64 cluster_update_trace_dropped_count(void);
extern bool cluster_update_trace_snapshot(uint32 slot, ClusterUpdateTraceSnapshot *snapshot);
extern void cluster_update_trace_event_at(const ClusterUpdateTraceEvent *event, uint64 now);
extern uint32 cluster_update_trace_event_count(void);
extern uint64 cluster_update_trace_event_dropped_count(void);
extern bool cluster_update_trace_event_snapshot(uint32 slot, ClusterUpdateTraceEvent *event);
extern void cluster_update_trace_interval_enter_at(ClusterUpdateTraceInterval *interval,
												   const ClusterUpdateTraceEvent *event,
												   uint64 now);
extern void cluster_update_trace_interval_leave_at(ClusterUpdateTraceInterval *interval,
												   bool finished, uint64 now);

static inline uint64
cluster_update_trace_now_ns(void)
{
	instr_time now;

	INSTR_TIME_SET_CURRENT(now);
	return (uint64)INSTR_TIME_GET_NANOSEC(now);
}

static inline void
cluster_update_trace_event(const ClusterUpdateTraceEvent *event)
{
	if (unlikely(cluster_update_trace_enabled))
		cluster_update_trace_event_at(event, cluster_update_trace_now_ns());
}

static inline void
cluster_update_trace_begin(ClusterUpdateTraceScope *scope)
{
	scope->active = false;
	if (unlikely(cluster_update_trace_enabled))
		cluster_update_trace_begin_at(scope, cluster_update_trace_now_ns());
}

static inline void
cluster_update_trace_end(ClusterUpdateTraceScope *scope, uint32 status, uint64 rows)
{
	if (scope->active)
		cluster_update_trace_end_at(scope, status, rows, cluster_update_trace_now_ns());
}

#endif /* !FRONTEND */
#endif /* CLUSTER_UPDATE_TRACE_H */
