/*-------------------------------------------------------------------------
 *
 * cluster_cancel_token.c
 *	  pgrac per-proc deadlock-cancel token (spec-5.9 D3).
 *
 *	  Identity-matched cancel consume that replaces the spec-2.24 global
 *	  boolean.  See cluster_cancel_token.h for the protocol contract.  The
 *	  match logic (cluster_cancel_token_consume_against) is split out as a pure
 *	  function so the unit test can exercise the 8.A non-misfire invariant (U4)
 *	  without a live PGPROC / shared memory.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cancel_token.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster builds.
 *	  Spec: spec-5.9-deadlock-victim-policy-cancel-robustness.md (FROZEN v1.0).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h" /* RecoveryInProgress */
#include "cluster/cluster_cancel_token.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lmd_wait_state.h"
#include "cluster/cluster_signal.h"
#include "port/atomics.h"
#include "storage/proc.h"
#include "storage/spin.h"

void
cluster_cancel_token_init(ClusterCancelToken *tok)
{
	if (tok == NULL)
		return;
	memset(tok, 0, sizeof(*tok));
	SpinLockInit(&tok->cancel_mutex);
}

/*
 * Per-backend reset (slot reuse).  Clears any pending token + marker but keeps
 * the spinlock initialized.  A leftover token from the previous owner must
 * never be honored by the new owner (Rule 8.A) — the matched-consume already
 * guarantees that, and the reset removes the stale state outright.
 */
void
cluster_cancel_token_reset(ClusterCancelToken *tok)
{
	if (tok == NULL)
		return;
	SpinLockAcquire(&tok->cancel_mutex);
	tok->installed = false;
	tok->request_id = 0;
	tok->cluster_epoch = 0;
	tok->wait_seq = 0;
	tok->cancel_id = 0;
	tok->marker_status = CLUSTER_CANCEL_MARKER_NONE;
	tok->marker_cancel_id = 0;
	tok->marker_seq = 0;
	SpinLockRelease(&tok->cancel_mutex);
}

void
cluster_cancel_token_install(struct PGPROC *target, uint64 request_id, uint64 cluster_epoch,
							 uint64 wait_seq, uint64 cancel_id)
{
	ClusterCancelToken *tok;

	if (target == NULL)
		return;
	tok = &target->cluster_cancel_token;

	SpinLockAcquire(&tok->cancel_mutex);
	tok->request_id = request_id;
	tok->cluster_epoch = cluster_epoch;
	tok->wait_seq = wait_seq;
	tok->cancel_id = cancel_id;
	tok->installed = true;
	SpinLockRelease(&tok->cancel_mutex);

	cluster_lmd_cancel_token_installed_count_inc(1);
}

bool
cluster_cancel_token_consume_against(ClusterCancelToken *tok, const ClusterLmdWaitStateSnapshot *ws,
									 uint64 current_epoch, bool hard_skip)
{
	bool match;
	uint8 marker;

	if (tok == NULL || ws == NULL)
		return false;

	SpinLockAcquire(&tok->cancel_mutex);

	if (!tok->installed) {
		SpinLockRelease(&tok->cancel_mutex);
		return false;
	}

	/*
	 * Rule 8.A — honor ONLY when the token still names the live wait: the same
	 * request_id + cluster_epoch + wait_seq the backend is currently blocked
	 * on, and the epoch is still current.  wait_seq is the monotonic ABA guard:
	 * after slot reuse the new owner's wait_seq differs, so a stale signal can
	 * never match.
	 */
	match = ws->active && tok->request_id == ws->request_id
			&& tok->cluster_epoch == ws->cluster_epoch && tok->wait_seq == ws->wait_seq
			&& tok->cluster_epoch == current_epoch;

	/*
	 * Decide the marker (one signal -> one decision):
	 *   match + cancellable -> CONSUMED (honor; caller raises 40P01)
	 *   match + HARD-skip   -> PROTECTED (refuse; the coordinator escalates to an
	 *                          alternate — never force-kill the unsafe victim)
	 *   no match            -> STALE_CLEARED (refuse; never 40P01)
	 */
	if (match && hard_skip)
		marker = CLUSTER_CANCEL_MARKER_PROTECTED;
	else if (match)
		marker = CLUSTER_CANCEL_MARKER_CONSUMED;
	else
		marker = CLUSTER_CANCEL_MARKER_STALE_CLEARED;

	tok->installed = false;
	tok->marker_status = marker;
	tok->marker_cancel_id = tok->cancel_id;
	tok->marker_seq++;

	SpinLockRelease(&tok->cancel_mutex);

	if (marker == CLUSTER_CANCEL_MARKER_CONSUMED)
		cluster_lmd_cancel_consumed_count_inc(1);
	else if (marker == CLUSTER_CANCEL_MARKER_PROTECTED)
		cluster_lmd_victim_protected_skip_count_inc(1);
	else
		cluster_lmd_cancel_stale_cleared_count_inc(1);

	/* Honor (40P01) ONLY on a cancellable match. */
	return (marker == CLUSTER_CANCEL_MARKER_CONSUMED);
}

bool
cluster_cancel_token_is_hard_skip(void)
{
	/* Recovery: aborting the startup/recovery process as a deadlock victim is
	 * unsafe.  (2PC-prepared txns never actively wait, so they are never an
	 * in-cycle waiter victim — V6.) */
	return RecoveryInProgress();
}

bool
cluster_cancel_token_consume(void)
{
	ClusterLmdWaitStateSnapshot ws;

	/*
	 * Gate on the lightweight signal flag (set by the async PROCSIG handler) so
	 * the common no-cancel path takes no spinlock.  Clear it here; the handler
	 * re-sets it if another cancel signal lands.
	 */
	if (!cluster_ges_cancel_pending)
		return false;
	cluster_ges_cancel_pending = false;

	if (MyProc == NULL)
		return false;

	/* The token was written + barrier'd before the signal that set the flag. */
	pg_read_barrier();

	if (!cluster_lmd_wait_state_read(&MyProc->cluster_lmd_wait, &ws))
		memset(&ws, 0, sizeof(ws)); /* not waiting -> active == false -> mismatch */

	return cluster_cancel_token_consume_against(&MyProc->cluster_cancel_token, &ws,
												cluster_epoch_get_current(),
												cluster_cancel_token_is_hard_skip());
}

ClusterCancelMarker
cluster_cancel_token_take_marker(struct PGPROC *p, uint64 *out_cancel_id, uint64 *out_seq)
{
	ClusterCancelToken *tok;
	ClusterCancelMarker m;

	if (p == NULL)
		return CLUSTER_CANCEL_MARKER_NONE;
	tok = &p->cluster_cancel_token;

	SpinLockAcquire(&tok->cancel_mutex);
	m = (ClusterCancelMarker)tok->marker_status;
	if (out_cancel_id != NULL)
		*out_cancel_id = tok->marker_cancel_id;
	if (out_seq != NULL)
		*out_seq = tok->marker_seq;
	tok->marker_status = CLUSTER_CANCEL_MARKER_NONE; /* read-and-clear */
	SpinLockRelease(&tok->cancel_mutex);

	return m;
}
