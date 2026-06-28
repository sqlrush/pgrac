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
	/*
	 * spec-5.16 (orphan-grant, Rule 8.A) — the waiter abandoned this request at the
	 * bounded GES timeout but kept the entry as a TOMBSTONE (deadline re-armed to a
	 * short TTL) instead of deleting it.  A late GRANT that lands on a tombstone is
	 * an ORPHAN (the master granted a waiter no backend wants); the reply handler
	 * auto-releases it back to the master rather than leaving it as a phantom holder.
	 * Distinguished from a retransmit-duplicate GRANT for a SUCCEEDED request, whose
	 * entry was DELETED on success (lookup miss -> dropped, no release).
	 */
	bool abandoned;
} GesReplyWaitEntry;

/* spec-5.16 — result of an atomic reply delivery (lookup + act under one lock). */
typedef enum GesReplyDeliverResult {
	GES_REPLY_DELIVER_NO_WAITER = 0, /* entry gone (succeeded/late) -> drop */
	GES_REPLY_DELIVER_WOKE,			 /* live waiter woken (normal grant/reject) */
	GES_REPLY_DELIVER_ORPHAN		 /* tombstone GRANT -> caller auto-releases */
} GesReplyDeliverResult;

/*
 * Shmem region lifecycle (mirror cluster_lmd_graph_shmem pattern).
 */
extern Size cluster_ges_reply_wait_shmem_size(void);
extern void cluster_ges_reply_wait_shmem_init(void);
extern void cluster_ges_reply_wait_shmem_register(void);

/*
 * spec-5.16 (Rule 8.A) — next node-global request_id (never 0).  The reply-wait
 * 5-tuple key requires request_id to be unique node-wide; the lock-acquire path
 * sources it here instead of a backend-local counter (which reused request_id=1
 * across backends and collided on the key).
 */
extern uint64 cluster_ges_reply_wait_next_request_id(void);

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
 * spec-5.16 — atomic reply delivery (lookup + act under the HTAB lock, race-free).
 *	NO_WAITER: no entry (succeeded/late retransmit-dup) -> caller drops.
 *	WOKE:	   live waiter found -> verdict stored + CV broadcast (== wake).
 *	ORPHAN:	   a GRANT landed on an abandoned tombstone -> entry removed; the caller
 *			   auto-releases the phantom holder back to the master.
 */
extern GesReplyDeliverResult cluster_ges_reply_wait_deliver(const GesReplyWaitKey *key,
															uint32 reply_opcode,
															uint32 reject_reason);

/*
 * spec-5.16 — abandon a wait entry at the bounded GES timeout instead of deleting
 * it: keep it as a tombstone with deadline re-armed to `tombstone_deadline` (swept
 * later) so a late GRANT can be recognized as an orphan and auto-released.  Returns
 * true if a GRANT had ALREADY been delivered (raced just ahead of the timeout): the
 * entry is removed and the caller must auto-release that grant itself.
 */
extern bool cluster_ges_reply_wait_mark_abandoned(const GesReplyWaitKey *key,
												  TimestampTz tombstone_deadline);

/*
 * Delete an entry by 5-tuple key.  Called by:
 *	 - the waiter after CV wake (normal path) or after timeout
 *	   (HC17:  timeout MUST delete entry before request_id slot reuse).
 *
 *	Idempotent — silently succeeds if entry already gone.
 */
extern void cluster_ges_reply_wait_delete(const GesReplyWaitKey *key);

/*
 * Sweep expired ABANDONED tombstones.  Walks the HTAB and deletes entries
 * that are abandoned (spec-5.16 orphan-grant) AND past their re-armed TTL
 * deadline, incrementing the sweep counter for each.  Wired to the LMON tick
 * (cluster_lmon.c) — it is the backstop that reclaims an orphan tombstone when
 * no late GRANT ever lands on it.
 *
 *	spec-5.16 (Rule 8.A):  it deliberately does NOT sweep a live (non-abandoned)
 *	waiter whose deadline merely elapsed — that backend is still sleeping on the
 *	entry's CV and owns its own teardown; removing it here would race a freed
 *	slot against the sleeper.  Only a tombstone (whose backend has already
 *	returned) is safe to reclaim.
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
