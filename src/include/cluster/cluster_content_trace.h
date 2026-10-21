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

typedef enum ClusterContentTraceWait {
	CLCONTENT_PCM, CLCONTENT_TX, CLCONTENT_PAIR, CLCONTENT_NWAITS
} ClusterContentTraceWait;

typedef struct ClusterContentTraceLock {
	BufferTag tag;
	int32 buffer;
	int32 mode;
	bool tag_known;
} ClusterContentTraceLock;

typedef struct ClusterContentTraceOwner {
	int32 pid;
	uint32 procno;
	int64 backend_start;
	uint64 revision;
	uint64 waits_with_locks, pair_attempts, pair_misses;
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
extern void cluster_content_trace_capture(const BufferTag *tag, ClusterContentTraceCut *cut);
/* Caller must have discharged its original service pin/owner obligations. */
extern void cluster_content_trace_emit(const ClusterWaitTrace *guard,
	const ClusterContentTraceCut *cut);
#endif
#endif
