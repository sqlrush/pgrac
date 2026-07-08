/*-------------------------------------------------------------------------
 *
 * cluster_marker_async.h
 *	  Small process-local async FSM for qvotec marker submit mailboxes.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_marker_async.h
 *
 * NOTES
 *	  The mailbox remains the existing request_seq/completion_seq/result
 *	  slot.  This wrapper changes only the waiting shape: submit publishes
 *	  a request and returns; the owning LMON tick polls completion without
 *	  sleeping (spec-2.29a — the pre-async 2ms pg_usleep spin inside the
 *	  LMON tick starved the CSSD heartbeat relay and caused false-DEAD
 *	  storms during cold formation, BUG-C1).
 *
 *	  The staged has_staged_event flag is the P1-1 bump-once contract: a
 *	  pre-bump caller (fail-stop fence / node-remove / join Phase-1) must
 *	  not re-enter its epoch-bump path while a stage is live.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_MARKER_ASYNC_H
#define CLUSTER_MARKER_ASYNC_H

#include "c.h"

#include <string.h>

#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/latch.h"

typedef enum ClusterMarkerAsyncState {
	CLUSTER_MARKER_ASYNC_IDLE = 0,
	CLUSTER_MARKER_ASYNC_SUBMITTED
} ClusterMarkerAsyncState;

typedef enum ClusterMarkerPollResult {
	CLUSTER_MARKER_POLL_IDLE = 0,
	CLUSTER_MARKER_POLL_PENDING,
	CLUSTER_MARKER_POLL_ACKED,
	CLUSTER_MARKER_POLL_TIMEOUT
} ClusterMarkerPollResult;

typedef enum ClusterMarkerAsyncKind {
	CLUSTER_MARKER_KIND_UNKNOWN = 0,
	CLUSTER_MARKER_KIND_FENCE_FAILSTOP,
	CLUSTER_MARKER_KIND_FENCE_NODE_REMOVED,
	CLUSTER_MARKER_KIND_JOIN_PREPARE,
	CLUSTER_MARKER_KIND_JOIN_COMMITTED,
	CLUSTER_MARKER_KIND_CLEAN_LEAVE_COMMITTING,
	CLUSTER_MARKER_KIND_CLEAN_LEAVE_COMMITTED,
	CLUSTER_MARKER_KIND_NODE_REMOVE_REMOVING,
	CLUSTER_MARKER_KIND_NODE_REMOVE_SHRUNK,
	CLUSTER_MARKER_KIND_NODE_REMOVE_REMOVED
} ClusterMarkerAsyncKind;

typedef struct ClusterMarkerAsync {
	ClusterMarkerAsyncState state;
	uint64 inflight_seq;
	uint64 deadline_us;
	TimestampTz submitted_at;
	ClusterMarkerAsyncKind kind;
	int32 target_node;

	/*
	 * Caller-owned staged publish/commit state.  While true, the owner must not
	 * re-enter the pre-bump path; it either submits/polls this record, publishes
	 * after ACK, or releases it after failure/timeout.
	 */
	bool has_staged_event;
	uint64 staged_expect_epoch;
} ClusterMarkerAsync;

static inline const char *
cluster_marker_async_kind_name(ClusterMarkerAsyncKind kind)
{
	switch (kind) {
	case CLUSTER_MARKER_KIND_FENCE_FAILSTOP:
		return "fence_failstop";
	case CLUSTER_MARKER_KIND_FENCE_NODE_REMOVED:
		return "fence_node_removed";
	case CLUSTER_MARKER_KIND_JOIN_PREPARE:
		return "join_prepare";
	case CLUSTER_MARKER_KIND_JOIN_COMMITTED:
		return "join_committed";
	case CLUSTER_MARKER_KIND_CLEAN_LEAVE_COMMITTING:
		return "clean_leave_committing";
	case CLUSTER_MARKER_KIND_CLEAN_LEAVE_COMMITTED:
		return "clean_leave_committed";
	case CLUSTER_MARKER_KIND_NODE_REMOVE_REMOVING:
		return "node_remove_removing";
	case CLUSTER_MARKER_KIND_NODE_REMOVE_SHRUNK:
		return "node_remove_shrunk";
	case CLUSTER_MARKER_KIND_NODE_REMOVE_REMOVED:
		return "node_remove_removed";
	case CLUSTER_MARKER_KIND_UNKNOWN:
	default:
		return "unknown";
	}
}

static inline void
cluster_marker_async_init(ClusterMarkerAsync *a)
{
	memset(a, 0, sizeof(*a));
	a->state = CLUSTER_MARKER_ASYNC_IDLE;
	a->target_node = -1;
}

static inline bool
cluster_marker_async_is_submitted(const ClusterMarkerAsync *a)
{
	return a != NULL && a->state == CLUSTER_MARKER_ASYNC_SUBMITTED;
}

static inline bool
cluster_marker_async_mailbox_busy(pg_atomic_uint64 *request_seq, pg_atomic_uint64 *completion_seq)
{
	return pg_atomic_read_u64(request_seq) != pg_atomic_read_u64(completion_seq);
}

static inline bool
cluster_marker_async_submit(ClusterMarkerAsync *a, pg_atomic_uint64 *request_seq,
							pg_atomic_uint64 *completion_seq, struct Latch *qvotec_latch,
							TimestampTz now, uint64 timeout_us, ClusterMarkerAsyncKind kind,
							int32 target_node)
{
	uint64 seq;

	Assert(a != NULL);
	Assert(request_seq != NULL);
	Assert(completion_seq != NULL);

	if (a->state == CLUSTER_MARKER_ASYNC_SUBMITTED)
		return true;
	if (cluster_marker_async_mailbox_busy(request_seq, completion_seq))
		return false;

	pg_write_barrier();
	seq = pg_atomic_add_fetch_u64(request_seq, 1);
	if (qvotec_latch != NULL)
		SetLatch(qvotec_latch);

	a->state = CLUSTER_MARKER_ASYNC_SUBMITTED;
	a->inflight_seq = seq;
	a->deadline_us = (uint64)now + timeout_us;
	a->submitted_at = now;
	a->kind = kind;
	a->target_node = target_node;
	return true;
}

static inline ClusterMarkerPollResult
cluster_marker_async_poll(ClusterMarkerAsync *a, pg_atomic_uint64 *completion_seq,
						  pg_atomic_uint32 *result_slot, TimestampTz now, uint32 *out_result,
						  uint64 *out_elapsed_us)
{
	uint64 elapsed;

	Assert(a != NULL);
	Assert(completion_seq != NULL);
	Assert(result_slot != NULL);

	if (out_result != NULL)
		*out_result = 0;
	if (out_elapsed_us != NULL)
		*out_elapsed_us = 0;
	if (a->state != CLUSTER_MARKER_ASYNC_SUBMITTED)
		return CLUSTER_MARKER_POLL_IDLE;

	if (pg_atomic_read_u64(completion_seq) == a->inflight_seq) {
		pg_read_barrier();
		if (out_result != NULL)
			*out_result = pg_atomic_read_u32(result_slot);
		elapsed
			= ((uint64)now > (uint64)a->submitted_at) ? ((uint64)now - (uint64)a->submitted_at) : 0;
		if (out_elapsed_us != NULL)
			*out_elapsed_us = elapsed;
		a->state = CLUSTER_MARKER_ASYNC_IDLE;
		return CLUSTER_MARKER_POLL_ACKED;
	}

	if ((uint64)now >= a->deadline_us) {
		elapsed
			= ((uint64)now > (uint64)a->submitted_at) ? ((uint64)now - (uint64)a->submitted_at) : 0;
		if (out_elapsed_us != NULL)
			*out_elapsed_us = elapsed;
		a->state = CLUSTER_MARKER_ASYNC_IDLE;
		return CLUSTER_MARKER_POLL_TIMEOUT;
	}

	return CLUSTER_MARKER_POLL_PENDING;
}

static inline void
cluster_marker_async_release_stage(ClusterMarkerAsync *a)
{
	if (a == NULL)
		return;
	a->state = CLUSTER_MARKER_ASYNC_IDLE;
	a->inflight_seq = 0;
	a->deadline_us = 0;
	a->submitted_at = 0;
	a->kind = CLUSTER_MARKER_KIND_UNKNOWN;
	a->target_node = -1;
	a->has_staged_event = false;
	a->staged_expect_epoch = 0;
}

#endif /* CLUSTER_MARKER_ASYNC_H */
