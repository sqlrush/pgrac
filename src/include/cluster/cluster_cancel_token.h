/*-------------------------------------------------------------------------
 *
 * cluster_cancel_token.h
 *	  pgrac per-proc deadlock-cancel token (spec-5.9 D3).
 *
 *	  Replaces the spec-2.24 global boolean cancel-consume model with an
 *	  identity-matched per-proc token so a stale / retransmitted cross-node
 *	  cancel that arrives after the backend slot (procno) was reused cannot
 *	  kill the wrong transaction (Rule 8.A — the spec-5.9 P0#1 fix).
 *
 *	  The victim-node LMD installs a token (wait identity + cancel_id) into the
 *	  target PGPROC under cancel_mutex, then SendProcSignal()s the backend.  The
 *	  backend consumes it at its wait point ONLY when the token matches its live
 *	  D1d wait-state (request_id + cluster_epoch + wait_seq) and the epoch is
 *	  still current; otherwise it clears the token WITHOUT raising 40P01.  Match
 *	  or mismatch, the backend leaves a marker (status + cancel_id + monotonic
 *	  seq) that the LMD reads-and-clears to emit the correlated CANCEL_ACK
 *	  (CONSUMED / NOT_WAITING) in spec-5.9 D5/D6.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cancel_token.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster builds.
 *	  Spec: spec-5.9-deadlock-victim-policy-cancel-robustness.md (FROZEN v1.0).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CANCEL_TOKEN_H
#define CLUSTER_CANCEL_TOKEN_H

#include "cluster/cluster_lmd_wait_state.h"
#include "storage/s_lock.h"

struct PGPROC;

/*
 * Marker the victim backend leaves for the LMD to turn into a CANCEL_ACK.
 *	CONSUMED      — backend matched the token and is aborting (40P01 fires).
 *	STALE_CLEARED — token did not match the live wait-state; cleared, no 40P01
 *	                (the victim already left the wait / slot was reused).
 */
typedef enum ClusterCancelMarker {
	CLUSTER_CANCEL_MARKER_NONE = 0,
	CLUSTER_CANCEL_MARKER_CONSUMED = 1,
	CLUSTER_CANCEL_MARKER_STALE_CLEARED = 2,
	/* spec-5.9 D7 — token matched but the victim is HARD-skip (unsafe-to-cancel:
	 * recovery / system / 2PC-prepared); it refused to honor, so the coordinator
	 * must escalate to an alternate (never force-kill the unsafe victim). */
	CLUSTER_CANCEL_MARKER_PROTECTED = 3,
} ClusterCancelMarker;

/*
 * Per-proc deadlock-cancel token.  One lives in every PGPROC
 * (proc.h: PGPROC.cluster_cancel_token).
 */
typedef struct ClusterCancelToken {
	slock_t cancel_mutex; /* guards every field below (cross-process) */
	bool installed;		  /* a cancel is pending consume */
	uint64 request_id;	  /* wait identity the cancel targets ... */
	uint64 cluster_epoch;
	uint64 wait_seq;
	uint64 cancel_id; /* coordinator correlation id (threads token/ACK) */
	/* Read-and-clear marker for the LMD -> CANCEL_ACK bridge. */
	uint8 marker_status;	 /* ClusterCancelMarker */
	uint64 marker_cancel_id; /* cancel_id the marker reports */
	uint64 marker_seq;		 /* monotonic; LMD dedups by last-observed seq */
} ClusterCancelToken;

/* Lifecycle — proc.c InitProcGlobal (init) / InitProcess (reset). */
extern void cluster_cancel_token_init(ClusterCancelToken *tok);
extern void cluster_cancel_token_reset(ClusterCancelToken *tok);

/*
 * Install a cancel token into *target (victim-node LMD).  Caller must
 * pg_write_barrier() + SendProcSignal() after this so the token is visible
 * before the backend observes the signal flag.
 */
extern void cluster_cancel_token_install(struct PGPROC *target, uint64 request_id,
										 uint64 cluster_epoch, uint64 wait_seq, uint64 cancel_id);

/*
 * Core consume against an explicit wait-state snapshot + current epoch.
 *	Returns true iff the token matched (caller honors with FAIL_DEADLOCK ->
 *	40P01) and stamps a CONSUMED marker;  on mismatch it clears the token,
 *	stamps a STALE_CLEARED marker, and returns false (NEVER 40P01).  Exposed for
 *	the unit test (U4) — it takes no globals.
 */
extern bool cluster_cancel_token_consume_against(ClusterCancelToken *tok,
												 const ClusterLmdWaitStateSnapshot *ws,
												 uint64 current_epoch, bool hard_skip);

/* True iff THIS backend is unsafe-to-cancel (HARD-skip) — currently recovery
 * (aborting the startup/recovery process as a deadlock victim is unsafe).  2PC-
 * prepared txns hold locks via a dummy proc and never actively wait, so they are
 * never an in-cycle waiter victim (V6). */
extern bool cluster_cancel_token_is_hard_skip(void);

/*
 * Backend wrapper — gated on the cluster_ges_cancel_pending signal flag; reads
 * MyProc's live wait-state and the current epoch, then delegates to
 * cluster_cancel_token_consume_against.  Returns true iff the caller must honor
 * the cancel (FAIL_DEADLOCK).
 */
extern bool cluster_cancel_token_consume(void);

/*
 * Observe-and-clear *p's consume/stale marker (victim-node LMD tick).  Returns
 * the marker status (NONE when nothing new) and fills *out_cancel_id + *out_seq.
 */
extern ClusterCancelMarker cluster_cancel_token_take_marker(struct PGPROC *p, uint64 *out_cancel_id,
															uint64 *out_seq);

#endif /* CLUSTER_CANCEL_TOKEN_H */
