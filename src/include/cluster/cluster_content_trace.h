/*-------------------------------------------------------------------------
 * cluster_content_trace.h
 *   Bounded, opt-in local content-lock/wait observations. Never authority.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CONTENT_TRACE_H
#define CLUSTER_CONTENT_TRACE_H

#include "cluster/cluster_wait_trace.h"

#ifdef USE_PGRAC_CLUSTER
#define CLUSTER_CONTENT_TRACE_LOCKS 32
#define CLUSTER_CONTENT_TRACE_OWNERS 8
#define CLUSTER_CONTENT_TRACE_PAIR_CYCLES 4
#define CLUSTER_CONTENT_TRACE_PAIR_REQUESTS 2

typedef enum ClusterContentTracePairPhase {
	CLPAIR_FIRST, CLPAIR_CONDITIONAL_SECOND, CLPAIR_RESOLVE_SECOND, CLPAIR_DONE
} ClusterContentTracePairPhase;

typedef enum ClusterContentTracePairEvent {
	CLPAIR_TARGET_BEGIN, CLPAIR_TARGET_RESULT, CLPAIR_ROUND_STEP,
	CLPAIR_CACHED_X_SAMPLE, CLPAIR_CONTENT_BUSY, CLPAIR_PCM_BLOCKED,
	CLPAIR_OBJECT_BLOCKED, CLPAIR_PREPARE_DRIFT, CLPAIR_ACTIVATE_WAIT,
	CLPAIR_ACTIVATE_DRIFT, CLPAIR_ACTIVATE_OBJECT, CLPAIR_DISPATCH,
	CLPAIR_STAGE_REJECT, CLPAIR_REMOTE_MASTER_DISPATCH, CLPAIR_NEVENTS
} ClusterContentTracePairEvent;

/* Actual outbound dispatch fields, not a reconstructed authority receipt.
 * stage_ok means only the existing local staging call accepted this dispatch;
 * neither a remote master nor a staged message proves a current transfer. */
typedef struct ClusterContentTracePairRequest {
	BufferTag tag;
	/* Physical snapshot generation is not the request/head binding identity. */
	uint64 physical_generation, r4, formation, master_session, request, base, authority;
	uint64 requester_connection, ingress_connection, wire_sender_connection;
	int32 requester, master, action, action_node;
	uint32 ordered_lane;
	uint8 observed_mode, target_mode, source_candidate, retain_pi_if_dirty;
	uint8 outcome, flags;
	bool stage_ok;
	uint32 leg, phase;
} ClusterContentTracePairRequest;

typedef enum ClusterContentTraceWait {
	CLCONTENT_PCM, CLCONTENT_TX, CLCONTENT_PAIR, CLCONTENT_NWAITS
} ClusterContentTraceWait;

typedef struct ClusterContentTraceLock {
	BufferTag tag;
	int32 buffer;
	int32 mode;
	bool tag_known;
} ClusterContentTraceLock;

typedef struct ClusterContentTracePairCycleData {
	uint64 invocation, cycle;
	ClusterContentTraceLock targets[2];
	uint64 events[2][CLPAIR_NEVENTS];
	int32 last_results[2][CLPAIR_NEVENTS];
	uint32 phase, nrequests, omitted_requests;
	bool finished, error, conditional_miss;
	ClusterContentTracePairRequest requests[CLUSTER_CONTENT_TRACE_PAIR_REQUESTS];
} ClusterContentTracePairCycleData;

typedef struct ClusterContentTraceOwner {
	int32 pid;
	uint32 procno;
	int32 backend_id;
	int64 backend_start;
	uint64 revision;
	uint64 waits_with_locks, pair_attempts, pair_misses;
	uint64 pair_invocations, pair_cycles, pair_omitted, pair_nested_omitted;
	uint32 npair_cycles;
	ClusterContentTracePairCycleData pair[CLUSTER_CONTENT_TRACE_PAIR_CYCLES];
	uint32 nlocks, omitted_locks, wait_mask;
	ClusterContentTraceLock locks[CLUSTER_CONTENT_TRACE_LOCKS];
	ClusterWaitTrace waits[CLCONTENT_NWAITS];
} ClusterContentTraceOwner;

/* A non-atomic, bounded cut. busy/changed/dead/omitted mean UNKNOWN, never
 * absence. No reader waits for a publisher, including ERROR cleanup. */
typedef struct ClusterContentTraceCut {
	uint32 scanned, busy, changed, dead, matches, nowners, omitted_owners;
	uint32 omitted_locks;
	ClusterContentTraceOwner owners[CLUSTER_CONTENT_TRACE_OWNERS];
} ClusterContentTraceCut;

extern void cluster_content_trace_shmem_register(void);
extern void cluster_content_trace_lock_changed(LWLock *lock);
extern void cluster_content_trace_wait_set(ClusterContentTraceWait kind,
	const ClusterWaitTrace *wait);
extern void cluster_content_trace_wait_clear(ClusterContentTraceWait kind);
extern void cluster_content_trace_error_cleanup(void);
extern void cluster_content_trace_pair(Buffer target, bool missed);
extern void cluster_content_trace_pair_begin(Buffer first, Buffer second);
extern void cluster_content_trace_pair_cycle(void);
extern void cluster_content_trace_pair_phase(ClusterContentTracePairPhase phase);
extern void cluster_content_trace_pair_end(void);
extern bool cluster_content_trace_pair_active(Buffer target);
extern void cluster_content_trace_pair_event(Buffer target,
	ClusterContentTracePairEvent event, int32 result);
extern void cluster_content_trace_pair_request(Buffer target,
	const ClusterContentTracePairRequest *request);
extern void cluster_content_trace_capture(const BufferTag *tag, ClusterContentTraceCut *cut);
/* Caller must have discharged its original service pin/owner obligations. */
extern void cluster_content_trace_emit(const ClusterWaitTrace *guard,
	const ClusterContentTraceCut *cut);
#endif
#endif
