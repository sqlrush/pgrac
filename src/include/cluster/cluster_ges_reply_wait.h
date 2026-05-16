/*-------------------------------------------------------------------------
 *
 * cluster_ges_reply_wait.h
 *	  Cross-node GES reply wait table — spec-2.23 D1.
 *
 *	  Backend-side reply correlation HTAB for the cross-node GES grant
 *	  pipeline.  When cluster_ges_send_request_and_wait() (spec-2.23 D2)
 *	  ships a GES_REQUEST to a remote master, it inserts an entry into
 *	  this HTAB keyed by a 5-tuple, then blocks on the entry's
 *	  ConditionVariable.  The reply handler looks up by the same 5-tuple
 *	  and wakes the entry.  Timeout sweep deletes stale entries before
 *	  the request_id slot is reused, so a late reply finds no matching
 *	  entry and is silently dropped (counter++).
 *
 *	  HC17 invariant (spec-2.23 §3.2):
 *	    - Key is 5-tuple {request_id, source_node_id, dest_node_id,
 *	      request_opcode, cluster_epoch}.
 *	    - request_opcode discriminates REQUEST vs RELEASE replies that
 *	      may share the same request_id slot.
 *	    - cluster_epoch guards reconfig races (post-reconfig peers must
 *	      not satisfy a wait posted before the epoch advance).
 *	    - On timeout, the wait entry is unconditionally deleted by the
 *	      caller before the request_id slot can be recycled.
 *	    - Late reply (lookup miss) is silently dropped + late-drop
 *	      counter++.  No ereport — handler context cannot ERROR.
 *
 *	  Skeleton phase (spec-2.23 Step 1 D1):  shmem region, HTAB, key /
 *	  entry struct, public API surface.  The send-side and reply-side
 *	  CV wait/wake loops land Step 2 D2 (alongside cluster_unit
 *	  T-reply-1..6).
 *
 *	  Spec: spec-2.23-cross-node-ges-bast-deadlock-production.md (FROZEN v0.3)
 *	  Design: docs/ges-lock-protocol-design.md v1.0
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ges_reply_wait.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  All symbols are backend-only (#ifndef FRONTEND) per L8 inheritance.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GES_REPLY_WAIT_H
#define CLUSTER_GES_REPLY_WAIT_H

#ifndef FRONTEND

#include "port/atomics.h"
#include "storage/condition_variable.h"
#include "utils/timestamp.h"

/*
 * Reply wait key — 5-tuple (HC17 spec-2.23 §3.2 / Q2 amend).
 *
 *	source_node_id is the local node (the node waiting for the reply).
 *	dest_node_id is the remote master where the GES_REQUEST was sent.
 *	request_opcode echoes the original GesRequestOpcode so REQUEST and
 *	RELEASE replies that share the same request_id slot do not collide.
 *	cluster_epoch is the local cluster_epoch at send time; replies
 *	carrying a different epoch on the envelope will not match this key.
 */
typedef struct GesReplyWaitKey {
	uint64 request_id;
	int32 source_node_id;
	int32 dest_node_id;
	uint32 request_opcode; /* GesRequestOpcode value at send time */
	uint64 cluster_epoch;
} GesReplyWaitKey;

StaticAssertDecl(sizeof(GesReplyWaitKey) == 32, "GesReplyWaitKey HTAB key 32-byte lock");

/*
 * Reply wait HTAB entry.
 *
 *	cv is per-entry so the reply handler can wake exactly one waiter.
 *	ready guards spurious wakeups (CV check predicate).  reject_reason
 *	carries the verdict (0 = GRANT, non-zero = GesRejectReason).
 *	deadline lets the timeout sweep find stale entries.
 *
 *	Step 1 ships the struct only; CV-driven wait/wake bodies land
 *	Step 2 D2 (cluster_ges_send_request_and_wait + reply handler wire).
 */
typedef struct GesReplyWaitEntry {
	GesReplyWaitKey key;  /* HTAB key (must be first; HASH_BLOBS) */
	ConditionVariable cv; /* per-entry wake target */
	uint32 reject_reason; /* set by reply handler; 0 = GRANT */
	uint32 reply_opcode;  /* set by reply handler (GesReplyOpcode) */
	TimestampTz deadline; /* timeout sweep gate; 0 means no timeout */
	bool ready;			  /* CV check predicate */
} GesReplyWaitEntry;

/*
 * Shmem region lifecycle (mirror cluster_lmd_graph_shmem pattern).
 */
extern Size cluster_ges_reply_wait_shmem_size(void);
extern void cluster_ges_reply_wait_shmem_init(void);
extern void cluster_ges_reply_wait_shmem_register(void);

/*
 * Insert a new wait entry keyed by HC17 5-tuple.
 *
 *	Returns the inserted entry on success (caller can then arm the CV
 *	via ConditionVariablePrepareToSleep + ConditionVariableTimedSleep).
 *	Returns NULL if the table is full (GUC cluster.ges_reply_wait_max_
 *	entries cap; SQLSTATE 53R71 fail-closed at caller).
 *	Returns NULL if a duplicate 5-tuple key is found (should not happen
 *	in normal flow — request_id is per-backend monotonic + dest_node
 *	rotates; duplicate indicates a programming bug, caller asserts).
 *
 *	Caller MUST pair every successful insert with cluster_ges_reply_wait_
 *	delete (in normal path, in timeout path, and in error paths) so the
 *	entry slot does not leak.
 */
extern GesReplyWaitEntry *cluster_ges_reply_wait_insert(const GesReplyWaitKey *key,
														TimestampTz deadline);

/*
 * Look up an entry by 5-tuple key.
 *
 *	Returns NULL if not found (late reply per HC17 — caller bumps the
 *	late-drop counter and returns).  Returns the entry on hit;  caller
 *	stores reply_opcode / reject_reason / ready=true and broadcasts CV.
 */
extern GesReplyWaitEntry *cluster_ges_reply_wait_lookup(const GesReplyWaitKey *key);

/*
 * Wake a waiter found by lookup.  Marks ready=true, stores opcode +
 * reject_reason, then broadcasts the per-entry CV.  Caller MUST hold
 * no spinlock when calling (CV broadcast may take a path lock).
 */
extern void cluster_ges_reply_wait_wake(GesReplyWaitEntry *entry, uint32 reply_opcode,
										uint32 reject_reason);

/*
 * Delete an entry by 5-tuple key.  Called by:
 *	 - the waiter after CV wake (normal path) or after timeout
 *	   (HC17:  timeout MUST delete entry before request_id slot reuse).
 *
 *	Idempotent — silently succeeds if entry already gone.
 */
extern void cluster_ges_reply_wait_delete(const GesReplyWaitKey *key);

/*
 * Sweep timed-out entries.  Walks the HTAB, deletes entries whose
 * deadline < now, and increments the sweep counter for each.  Called
 * by the LMON tick (Step 2 D2 wires).  No-op in Step 1.
 *
 *	Returns the number of entries swept.
 */
extern int cluster_ges_reply_wait_sweep_timeout(TimestampTz now);

/*
 * Counter accessors (cluster_debug dump_ges surface — spec-2.23 D13
 * dump_ges 2→5 baseline ripple).
 */
extern uint64 cluster_ges_reply_wait_table_active_count(void);
extern uint64 cluster_ges_reply_late_drop_count(void);
extern uint64 cluster_ges_release_ack_count(void);
extern void cluster_ges_inc_release_ack(void);
extern void cluster_ges_inc_reply_late_drop(void);

#endif /* !FRONTEND */

#endif /* CLUSTER_GES_REPLY_WAIT_H */
