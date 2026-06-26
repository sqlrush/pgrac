/*-------------------------------------------------------------------------
 *
 * cluster_hang_resolve_policy.c
 *	  pgrac Hang Manager disposition (spec-5.12) — pure decision layer.
 *
 *	  Every function here is pure: it decides, scores, classifies, or walks
 *	  a caller-provided snapshot / map, with no PostgreSQL runtime
 *	  dependency (no shmem, no locks, no pgstat, no ereport, no palloc).
 *	  That keeps the fail-CLOSED disposition logic exercisable directly by
 *	  cluster_unit (test_cluster_hang_resolve) and free of the cross-process
 *	  publish-read windows that the runtime layer must guard.
 *
 *	  The runtime layer (cluster_hang_resolve.c) gathers the facts (pgstat +
 *	  lock snapshots, the 5.8 WFG probe, the confirmation map in the DIAG
 *	  shmem region) and feeds them here, then acts on the verdict.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hang_resolve_policy.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.12-hang-manager-disposition.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>
#include <signal.h>

#include "cluster/cluster_hang_resolve.h"
#include "miscadmin.h" /* B_BACKEND */


/* ============================================================
 * Victim score (D2)
 * ============================================================ */

/*
 * cluster_hang_victim_score -- higher = dispose first.
 *
 *	score = w_age      * log(1 + xact_age_seconds)
 *	      + w_rollback * (-log(1 + n_locks_held))
 *	      + w_blockers * n_blocked
 *
 *	Older transactions, fewer held locks (cheaper rollback), and more
 *	blocked waiters all raise the score.  priority / cpu_time are NOT
 *	factors: PostgreSQL exposes no per-session priority or per-backend CPU
 *	accounting, so pretending would violate the Oracle-honesty rule
 *	(spec §8 Q7).
 */
double
cluster_hang_victim_score(const ClusterHangVictim *v, double w_age, double w_rollback,
						  double w_blockers)
{
	double age_s = (v->xact_age_us > 0) ? ((double)v->xact_age_us / 1000000.0) : 0.0;
	int n_locks = (v->n_locks_held > 0) ? v->n_locks_held : 0;
	int n_blocked = (v->n_blocked > 0) ? v->n_blocked : 0;

	return w_age * log1p(age_s) + w_rollback * (-log1p((double)n_locks))
		   + w_blockers * (double)n_blocked;
}

/*
 * cluster_hang_victim_score_cmp -- deterministic ordering for qsort.
 *
 *	Primary: score DESC (highest-score victim first).  Ties break by
 *	wait_age DESC, then n_blocked DESC, then pid ASC -- fully deterministic,
 *	no randomness (spec §2.2 / L4).  Returns <0 when a should be disposed
 *	before b.
 */
int
cluster_hang_victim_score_cmp(const ClusterHangVictim *a, const ClusterHangVictim *b)
{
	if (a->score > b->score)
		return -1;
	if (a->score < b->score)
		return 1;
	if (a->wait_age_us > b->wait_age_us)
		return -1;
	if (a->wait_age_us < b->wait_age_us)
		return 1;
	if (a->n_blocked > b->n_blocked)
		return -1;
	if (a->n_blocked < b->n_blocked)
		return 1;
	if (a->pid < b->pid)
		return -1;
	if (a->pid > b->pid)
		return 1;
	return 0;
}


/* ============================================================
 * HARD-skip / lock-holder classification (D2)
 * ============================================================ */

/*
 * cluster_hang_victim_skip_reason -- never-kill classification.
 *
 *	The first four results are the HARD-skip never-kill set (recovery,
 *	non-regular backend, 2PC-prepared, operator-critical).  NOT_LOCK_HOLDER
 *	means the candidate is not currently granted-holding the conflicting
 *	lock, so it is not a valid victim (G-lock-holder, spec §3.2).  Note
 *	is_lock_holder is the gate, NOT having an xid: an idle-in-tx
 *	"BEGIN; LOCK TABLE t;" blocker holds the lock with no xid and is still a
 *	valid victim (spec v0.3 P1#1).
 */
ClusterHangVictimSkip
cluster_hang_victim_skip_reason(int backend_type, bool is_recovery, bool is_prepared_2pc,
								bool is_critical, bool is_lock_holder)
{
	if (is_recovery)
		return HANG_VICTIM_SKIP_RECOVERY;
	if (backend_type != B_BACKEND)
		return HANG_VICTIM_SKIP_SYSTEM;
	if (is_prepared_2pc)
		return HANG_VICTIM_SKIP_2PC;
	if (is_critical)
		return HANG_VICTIM_SKIP_CRITICAL;
	if (!is_lock_holder)
		return HANG_VICTIM_SKIP_NOT_LOCK_HOLDER;
	return HANG_VICTIM_OK;
}


/* ============================================================
 * fail-CLOSED gate (D4)
 * ============================================================ */

/*
 * cluster_hang_gate_decide -- default-deny disposition gate.
 *
 *	Ordering matches spec §3.2: G-actionable, then G-over-exclude (5.8 WFG),
 *	then G-hard-skip, then G-lock-holder, then G-confirm.  Any non-PASS
 *	result names the gate that rejected the candidate.  G-ABA (the pre-signal
 *	re-validate) is a separate runtime step that re-checks identity at the
 *	instant of signalling; it is not part of this pure verdict.
 */
ClusterHangGateResult
cluster_hang_gate_decide(bool actionable, bool in_wfg, ClusterHangVictimSkip skip, bool confirmed)
{
	if (!actionable)
		return HANG_GATE_NOT_ACTIONABLE;
	if (in_wfg)
		return HANG_GATE_OVER_EXCLUDED;

	switch (skip) {
	case HANG_VICTIM_SKIP_SYSTEM:
	case HANG_VICTIM_SKIP_RECOVERY:
	case HANG_VICTIM_SKIP_2PC:
	case HANG_VICTIM_SKIP_CRITICAL:
		return HANG_GATE_HARD_SKIP;
	case HANG_VICTIM_SKIP_NOT_LOCK_HOLDER:
		return HANG_GATE_NOT_LOCK_HOLDER;
	case HANG_VICTIM_OK:
		break;
	}

	if (!confirmed)
		return HANG_GATE_NOT_CONFIRMED;
	return HANG_GATE_PASS;
}

/*
 * cluster_hang_counter_note_gate_reject -- bump the counter matching a gate
 * rejection.  PASS and NOT_LOCK_HOLDER have no dedicated counter (spec §2.6)
 * and bump nothing.  Caller holds the DIAG lock.
 */
void
cluster_hang_counter_note_gate_reject(ClusterHangResolveCounters *c, ClusterHangGateResult r)
{
	switch (r) {
	case HANG_GATE_NOT_ACTIONABLE:
		c->non_actionable_skipped++;
		break;
	case HANG_GATE_OVER_EXCLUDED:
		c->over_excluded++;
		break;
	case HANG_GATE_HARD_SKIP:
		c->hard_skipped++;
		break;
	case HANG_GATE_NOT_CONFIRMED:
		c->not_confirmed_yet++;
		break;
	case HANG_GATE_PASS:
	case HANG_GATE_NOT_LOCK_HOLDER:
		break;
	}
}


/* ============================================================
 * tier -> signal (D3, default-deny)
 * ============================================================ */

/*
 * cluster_hang_tier_signal -- map a disposition tier to the OS signal.
 *
 *	CANCEL -> SIGINT, TERMINATE -> SIGTERM.  NONE, DEGRADE, and any unknown
 *	tier map to 0 (no signal): an unrecognised tier never "falls through"
 *	into a destructive signal (spec §3.2 L237).  SIGKILL is never produced
 *	(see cluster_hang_resolve.h header).
 */
int
cluster_hang_tier_signal(ClusterHangActionTier tier)
{
	switch (tier) {
	case HANG_ACTION_CANCEL:
		return SIGINT;
	case HANG_ACTION_TERMINATE:
		return SIGTERM;
	case HANG_ACTION_NONE:
	case HANG_ACTION_DEGRADE:
		return 0;
	}
	return 0; /* default-deny for any out-of-range tier value */
}


/* ============================================================
 * mode predicates (D1)
 * ============================================================ */

bool
cluster_hang_mode_evaluates(ClusterHangResolveMode mode)
{
	return mode == HANG_RESOLVE_ADVISORY || mode == HANG_RESOLVE_ENFORCE;
}

bool
cluster_hang_mode_dispatches(ClusterHangResolveMode mode)
{
	return mode == HANG_RESOLVE_ENFORCE;
}


/* ============================================================
 * root-first blocker ascent (D2)
 * ============================================================ */

/*
 * cluster_hang_root_blocker_pid -- ascend blocker_pid links to the root.
 *
 *	From start_slot, follow blocker_pid while the blocker is itself a sampled
 *	long-waiter, and stop at the first blocker that is NOT a sampled waiter
 *	(the idle-in-tx root holder that does not wait), returning its pid.
 *	Killing the root unblocks the whole chain; killing a waiter is pointless.
 *
 *	Cycle-guarded (actionable already excludes confirmed deadlocks, but a
 *	torn snapshot could still show a cycle) and depth-bounded.  Sets
 *	*truncated when a cycle or the max_depth limit cut the walk short, and
 *	returns the last blocker pid seen.  Returns -1 when start_slot has no
 *	blocker.
 */
int
cluster_hang_root_blocker_pid(const ClusterHangSampleStore *store, int start_slot, int max_depth,
							  bool *truncated)
{
	int visited[CLUSTER_HANG_MAX_SAMPLES];
	int n_visited = 0;
	int cur = start_slot;
	int depth;

	if (truncated)
		*truncated = false;
	if (store == NULL || start_slot < 0 || start_slot >= store->n_samples)
		return -1;

	for (depth = 0;; depth++) {
		int bp = store->slots[cur].blocker_pid;
		int next = -1;
		int j;
		int k;

		if (bp < 0)
			return -1; /* this node has no blocker */

		/* is the blocker itself a sampled long-waiter? */
		for (j = 0; j < store->n_samples; j++) {
			if (store->slots[j].pid == bp) {
				next = j;
				break;
			}
		}
		if (next < 0)
			return bp; /* blocker is not sampled -> idle root holder */

		/* must ascend one more hop -- enforce the depth bound */
		if (depth >= max_depth) {
			if (truncated)
				*truncated = true;
			return bp;
		}

		/* cycle guard: would ascending revisit the start or a prior node? */
		if (next == start_slot) {
			if (truncated)
				*truncated = true;
			return bp;
		}
		for (k = 0; k < n_visited; k++) {
			if (visited[k] == next) {
				if (truncated)
					*truncated = true;
				return bp;
			}
		}
		if (n_visited < CLUSTER_HANG_MAX_SAMPLES)
			visited[n_visited++] = cur;
		cur = next;
	}
}


/* ============================================================
 * confirmation / hysteresis + tier escalation (D5)
 * ============================================================ */

void
cluster_hang_confirm_begin_round(ClusterHangConfirmMap *map)
{
	int i;

	for (i = 0; i < CLUSTER_HANG_CONFIRM_MAX; i++)
		map->entries[i].seen_this_round = false;
}

/*
 * cluster_hang_confirm_touch -- record that identity (pid, backendId) is an
 * actionable long-wait candidate this round.  Increments the consecutive
 * round count the first time the identity is touched in a round (idempotent
 * within a round).  Allocates a fresh entry (consecutive=1) for a new
 * identity; returns NULL only if the map is full.
 */
ClusterHangConfirmEntry *
cluster_hang_confirm_touch(ClusterHangConfirmMap *map, int pid, int backendId)
{
	int i;
	int free_idx = -1;
	ClusterHangConfirmEntry *e;

	for (i = 0; i < CLUSTER_HANG_CONFIRM_MAX; i++) {
		e = &map->entries[i];
		if (e->in_use && e->pid == pid && e->backendId == backendId) {
			if (!e->seen_this_round) {
				e->consecutive_rounds++;
				e->seen_this_round = true;
			}
			return e;
		}
		if (!e->in_use && free_idx < 0)
			free_idx = i;
	}

	if (free_idx < 0)
		return NULL; /* map full */

	e = &map->entries[free_idx];
	e->in_use = true;
	e->seen_this_round = true;
	e->pid = pid;
	e->backendId = backendId;
	e->consecutive_rounds = 1;
	e->last_action_tier = HANG_ACTION_NONE;
	e->last_action_at = 0;
	return e;
}

/*
 * cluster_hang_confirm_end_round -- reset entries not touched this round.
 *
 *	An identity that was not seen this round is no longer an actionable
 *	long-wait (it self-resolved, changed wait, or vanished), so its
 *	confirmation history is cleared -- a later reappearance counts as a fresh
 *	start, never resuming a stale streak.
 */
void
cluster_hang_confirm_end_round(ClusterHangConfirmMap *map)
{
	int i;

	for (i = 0; i < CLUSTER_HANG_CONFIRM_MAX; i++) {
		ClusterHangConfirmEntry *e = &map->entries[i];

		if (e->in_use && !e->seen_this_round) {
			e->in_use = false;
			e->consecutive_rounds = 0;
			e->last_action_tier = HANG_ACTION_NONE;
			e->last_action_at = 0;
		}
	}
}

bool
cluster_hang_confirm_ready(const ClusterHangConfirmEntry *e, int confirm_rounds)
{
	/* Caller must pass a non-NULL entry (e.g. guard touch()'s result first). */
	return e->consecutive_rounds >= confirm_rounds;
}

/*
 * cluster_hang_confirm_decide_tier -- which tier to apply this round.
 *
 *	Escalation is cross-round and non-blocking (DIAG never sleeps waiting for
 *	a victim to react, spec §2.3).  From the last action: NONE -> CANCEL;
 *	CANCEL -> TERMINATE once the soft-timeout grace elapsed, else NONE
 *	(anti-thrash: do not re-signal within the grace window); TERMINATE ->
 *	DEGRADE once the grace elapsed, else NONE; DEGRADE is terminal.
 */
ClusterHangActionTier
cluster_hang_confirm_decide_tier(const ClusterHangConfirmEntry *e, TimestampTz now,
								 int soft_timeout_ms)
{
	int64 grace_us = (int64)soft_timeout_ms * 1000;
	int64 elapsed = now - e->last_action_at;

	switch ((ClusterHangActionTier)e->last_action_tier) {
	case HANG_ACTION_NONE:
		return HANG_ACTION_CANCEL;
	case HANG_ACTION_CANCEL:
		return (elapsed >= grace_us) ? HANG_ACTION_TERMINATE : HANG_ACTION_NONE;
	case HANG_ACTION_TERMINATE:
		return (elapsed >= grace_us) ? HANG_ACTION_DEGRADE : HANG_ACTION_NONE;
	case HANG_ACTION_DEGRADE:
		return HANG_ACTION_DEGRADE;
	}
	return HANG_ACTION_DEGRADE; /* default-deny: unknown last tier -> terminal */
}

void
cluster_hang_confirm_record_action(ClusterHangConfirmEntry *e, ClusterHangActionTier tier,
								   TimestampTz now)
{
	e->last_action_tier = (uint8)tier;
	e->last_action_at = now;
}


/* ============================================================
 * String helpers (dumps + logs)
 * ============================================================ */

const char *
cluster_hang_resolve_mode_str(int mode)
{
	switch (mode) {
	case HANG_RESOLVE_OFF:
		return "off";
	case HANG_RESOLVE_ADVISORY:
		return "advisory";
	case HANG_RESOLVE_ENFORCE:
		return "enforce";
	default:
		return "(unknown)";
	}
}

const char *
cluster_hang_action_tier_str(int tier)
{
	switch (tier) {
	case HANG_ACTION_NONE:
		return "none";
	case HANG_ACTION_CANCEL:
		return "cancel";
	case HANG_ACTION_TERMINATE:
		return "terminate";
	case HANG_ACTION_DEGRADE:
		return "degrade";
	default:
		return "(unknown)";
	}
}
