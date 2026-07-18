/*-------------------------------------------------------------------------
 *
 * cluster_lmd_wait_state.h
 *	  pgrac per-proc cluster wait-state (spec-5.8 D1d).
 *
 *	  A backend blocked in a cross-node lock wait publishes a small
 *	  wait-state record before it sleeps and clears it on every exit.  The
 *	  LMD deadlock resolver reads this record cross-process (by procno) to
 *	  revalidate that a chosen victim is still genuinely waiting on the same
 *	  request before it cancels the victim (spec-5.8 D5).  Without it the
 *	  resolver has no authoritative "is this backend still waiting" signal
 *	  once master-side edges (spec-5.8 D1b) move the wait-for edge off the
 *	  victim's own node.
 *
 *	  The record is embedded in PGPROC (no new shmem region).  wait_seq is a
 *	  per-proc monotonic counter that is never reset on backend reuse, so a
 *	  stale (request_id, cluster_epoch, wait_seq) tuple recorded for a
 *	  previous backend at the same procno can never match a new wait — this
 *	  is the ABA guard against procno reuse.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_lmd_wait_state.h
 *
 * NOTES
 *	  Leaf header: depends only on port/atomics.h and base postgres types.
 *	  Safe to include from storage/proc.h (no back-reference to PGPROC).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LMD_WAIT_STATE_H
#define CLUSTER_LMD_WAIT_STATE_H

#include "port/atomics.h"

/*
 * Kind of cluster wait a backend is blocked in.  Recorded so the resolver
 * can distinguish a GES reply wait from a TX enqueue wait when it builds the
 * victim's identity.
 */
typedef enum ClusterLmdWaitKind {
	CLUSTER_LMD_WAIT_NONE = 0,
	CLUSTER_LMD_WAIT_GES = 1,		  /* GES REQUEST / CONVERT reply wait */
	CLUSTER_LMD_WAIT_TX = 2,		  /* cross-node TX enqueue wait */
	CLUSTER_LMD_WAIT_PCM_CONVERT = 3, /* PCM X convert-queue wait */
} ClusterLmdWaitKind;

/*
 * Per-proc cluster wait-state, embedded in PGPROC.  Written only by the
 * owning backend; read cross-process by the LMD resolver.  change_seq is a
 * seqlock counter: the owner makes it odd before changing any payload field
 * and even after the complete tuple is stable.  A reader accepts a snapshot
 * only when equal even values bracket its payload copy.  Therefore clear +
 * republish cannot combine fields from different wait generations.
 *
 * active consumes the four alignment bytes that already preceded wait_seq,
 * keeping this PGPROC-embedded record at 48 bytes without moving any payload
 * field.  wait_seq remains the monotonic per-wait ABA identity and is
 * separate from the structural seqlock counter.
 */
typedef struct ClusterLmdProcWaitState {
	pg_atomic_uint32 change_seq; /* odd = payload update in progress */
	uint32 active;				 /* 1 = blocked in a cluster wait */
	pg_atomic_uint64 wait_seq;	 /* monotonic publish counter (ABA guard) */
	uint8 kind;					 /* ClusterLmdWaitKind */
	uint64 request_id;			 /* GES request id / TX waiter id */
	uint64 cluster_epoch;		 /* epoch at wait-enter */
	TransactionId xid;			 /* waiter xid (InvalidTransactionId allowed) */
} ClusterLmdProcWaitState;

StaticAssertDecl(sizeof(ClusterLmdProcWaitState) == 48,
				 "ClusterLmdProcWaitState PGPROC ABI must stay 48 bytes");
StaticAssertDecl(offsetof(ClusterLmdProcWaitState, change_seq) == 0,
				 "ClusterLmdProcWaitState change_seq offset");
StaticAssertDecl(offsetof(ClusterLmdProcWaitState, active) == 4,
				 "ClusterLmdProcWaitState active offset");
StaticAssertDecl(offsetof(ClusterLmdProcWaitState, wait_seq) == 8,
				 "ClusterLmdProcWaitState wait_seq offset");
StaticAssertDecl(offsetof(ClusterLmdProcWaitState, kind) == 16,
				 "ClusterLmdProcWaitState kind offset");
StaticAssertDecl(offsetof(ClusterLmdProcWaitState, request_id) == 24,
				 "ClusterLmdProcWaitState request_id offset");
StaticAssertDecl(offsetof(ClusterLmdProcWaitState, cluster_epoch) == 32,
				 "ClusterLmdProcWaitState cluster_epoch offset");
StaticAssertDecl(offsetof(ClusterLmdProcWaitState, xid) == 40,
				 "ClusterLmdProcWaitState xid offset");

/* Read-side snapshot handed to the resolver. */
typedef struct ClusterLmdWaitStateSnapshot {
	bool active;
	uint8 kind;
	uint64 request_id;
	uint64 cluster_epoch;
	TransactionId xid;
	uint64 wait_seq;
} ClusterLmdWaitStateSnapshot;

/*
 * Exact read outcome.  INACTIVE means a stable even seqlock snapshot proved
 * active=false.  BUSY means the bounded reader could not obtain any stable
 * snapshot and must not be treated as evidence that the proc is inactive.
 */
typedef enum ClusterLmdWaitStateReadResult {
	CLUSTER_LMD_WAIT_STATE_READ_INACTIVE = 0,
	CLUSTER_LMD_WAIT_STATE_READ_ACTIVE = 1,
	CLUSTER_LMD_WAIT_STATE_READ_BUSY = 2,
} ClusterLmdWaitStateReadResult;

/*
 * InitProcGlobal — full zero of the record (once per proc slot at postmaster
 * start).  Establishes wait_seq = 0 so the first publish yields wait_seq = 1.
 */
extern void cluster_lmd_wait_state_init(ClusterLmdProcWaitState *ws);

/*
 * InitProcess — publish an inactive stable tuple for a freshly assigned
 * backend WITHOUT resetting wait_seq.  Preserving wait_seq across backend
 * reuse is what makes the ABA guard work; reset also closes an odd seqlock
 * left by a predecessor that died midway through publishing.
 */
extern void cluster_lmd_wait_state_reset(ClusterLmdProcWaitState *ws);

/*
 * Publish the wait-state before entering a cluster wait.  Bumps wait_seq and
 * sets active; returns the new wait_seq so the caller can stamp it on the WFG
 * vertex it submits (spec-5.8 D1e), letting the resolver match it back here.
 */
extern uint64 cluster_lmd_wait_state_publish(ClusterLmdProcWaitState *ws, uint8 kind,
											 uint64 request_id, uint64 cluster_epoch,
											 TransactionId xid);

/* Clear on every wait exit path (grant / timeout / ERROR / cancel). */
extern void cluster_lmd_wait_state_clear(ClusterLmdProcWaitState *ws);

/*
 * Tri-state cross-process read.  ACTIVE fills the complete stable tuple;
 * INACTIVE is returned only after a stable even read proves active=false;
 * BUSY reports bounded retry exhaustion on an in-progress/torn writer.
 * *out is zeroed for both non-ACTIVE results.
 */
extern ClusterLmdWaitStateReadResult
cluster_lmd_wait_state_read_exact(ClusterLmdProcWaitState *ws, ClusterLmdWaitStateSnapshot *out);

/*
 * Cross-process read by the resolver.  Returns true and fills *out when the
 * proc is actively waiting; returns false (out->active = false) when inactive
 * or when a bounded number of retries cannot obtain a stable even snapshot.
 */
extern bool cluster_lmd_wait_state_read(ClusterLmdProcWaitState *ws,
										ClusterLmdWaitStateSnapshot *out);

#endif /* CLUSTER_LMD_WAIT_STATE_H */
