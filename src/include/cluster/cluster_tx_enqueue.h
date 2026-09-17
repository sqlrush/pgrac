/*-------------------------------------------------------------------------
 *
 * cluster_tx_enqueue.h
 *	  pgrac cross-node TX enqueue completion wait (Oracle TX enqueue model).
 *
 *	  spec-5.2 D4/D6.  Replaces the spec-3.4d fail-closed (53R98) for a
 *	  remote row lock with a real completion wait:  a backend blocks until
 *	  the remote holder transaction completes (commit/abort) or a finite
 *	  timeout elapses, then re-judges.  The row lock itself lives in the
 *	  tuple (xmax / ITL) — this layer only records the WAITING relationship
 *	  and wakes the waiter when the holder's TT status becomes terminal.
 *
 *	  Identity discipline (Rule 8.A / L249, spec-5.2 G1 + H1):  a waiter is
 *	  keyed by the FULL 24B ClusterTTStatusKey of the holder (origin_node_id
 *	  = the HOLDER node, plus undo_segment_id + tt_slot_id + cluster_epoch +
 *	  local_xid).  A raw TransactionId alone is not a cluster identity, and
 *	  the generic cluster_grd_resid_encode (which forces field2 = the LOCAL
 *	  node) must NOT be used — that would key on the requester's node and
 *	  never match the holder.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_tx_enqueue.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.2-cf-liveread-dataplane-and-tx-row-lock-wait.md (D4/D6)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TX_ENQUEUE_H
#define CLUSTER_TX_ENQUEUE_H

#include "cluster/cluster_tt_status.h" /* ClusterTTStatusKey */
#include "cluster/cluster_tx_resolve.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * Result of a completion wait.  The caller MUST re-judge (re-read xmax) on
 * RESOLVED and DEAD_HOLDER — this layer never returns a visibility verdict.
 */
typedef enum ClusterTxwResult {
	CLUSTER_TXW_RESOLVED = 0, /* holder committed/aborted — re-judge */
	CLUSTER_TXW_TIMEOUT,	  /* finite timeout elapsed — caller ERRORs (53R70) */
	CLUSTER_TXW_DEAD_HOLDER,  /* holder node fenced / TT ABORTED — re-judge */
	/* A PCM-X freeze made sleeping under another held content lock unsafe.
	 * Process-local only: callers must retry/fail before any terminal handler. */
	CLUSTER_TXW_RETRY,
	/* Exact TARGET authority could not be proved; reason_out is non-NONE. */
	CLUSTER_TXW_UNPROVABLE,
	/* Matching LMD cancel token consumed while the exact wait remained live.
	 * The wait function returns this only after slot/state/edge cleanup. */
	CLUSTER_TXW_DEADLOCK
} ClusterTxwResult;

/*
 * cluster_tx_enqueue_wait — block until the remote holder transaction
 * identified by holder_key completes, or until effective_timeout_ms elapses.
 *
 *   holder_key:  full 24B key of the HOLDER (origin_node_id = holder node).
 *   effective_timeout_ms:  finite wait budget; values <= 0 are clamped to a
 *     finite default (perpetual -1 is forbidden, Q6 / 5.1b clause 8).
 *
 * Returns RESOLVED / TIMEOUT / DEAD_HOLDER / RETRY / DEADLOCK.  RETRY means
 * the caller must leave the current PCM-X freeze window before attempting
 * another wait.  DEADLOCK means matching-token consumption and all wait
 * cleanup have completed; the immediate caller maps it to SQLSTATE 40P01.
 * Does not change any tuple.
 */
extern ClusterTxwResult cluster_tx_enqueue_wait(const ClusterTTStatusKey *holder_key,
												int effective_timeout_ms);

/* R4 D9 exact TARGET wait.  The caller owns locator capture and mandatory
 * post-wait page/tuple requalification; this layer never returns visibility. */
extern ClusterTxwResult cluster_tx_enqueue_wait_exact(const ClusterTxLocator *locator,
													  int effective_timeout_ms,
													  ClusterTxResolveReason *reason_out);

/* Idempotent R4 D9 SOURCE/TARGET-wait cleanup for proc_exit/FATAL paths. */
extern void cluster_tx_enqueue_cleanup_on_backend_exit_callback(int code, Datum arg);

/*
 * Current-DML MultiXact uses the same TX wait semantics and WFG integration,
 * but active R4 never consults the dormant SOURCE row.  It waits for one
 * bounded native wake/poll slice, returns RETRY after full cleanup, and lets
 * the heap caller re-DESCRIBE exact TARGET authority.  The caller retains the
 * in/out monotonic deadline across those retries; zero initializes it once.
 */
extern ClusterTxwResult cluster_tx_enqueue_wait_current_mx(const ClusterTTStatusKey *holder_key,
														   int effective_timeout_ms,
														   uint64 *absolute_deadline_mono_us);

/*
 * cluster_txw_wake_waiters — wake every backend waiting on holder_key.
 *
 *   Called from the TT-status-hint apply path when a holder transaction's
 *   status becomes terminal (COMMITTED/ABORTED), and from the dead-holder /
 *   reconfig fence path.  Matches on the FULL 24B key (H1):  the same
 *   (origin, xid, epoch) with a different undo_segment_id / tt_slot_id (slot
 *   reuse) must NOT be woken.
 */
extern void cluster_txw_wake_waiters(const ClusterTTStatusKey *holder_key);

/* Shmem region lifecycle (registered from cluster_shmem.c). */
extern Size cluster_tx_enqueue_shmem_size(void);
extern void cluster_tx_enqueue_shmem_init(void);
extern void cluster_tx_enqueue_shmem_register(void);

/* Counters (D9 observability). */
extern uint64 cluster_txw_get_wait_count(void);
extern uint64 cluster_txw_get_wakeup_count(void);
extern uint64 cluster_txw_get_timeout_count(void);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_TX_ENQUEUE_H */
