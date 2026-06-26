/*-------------------------------------------------------------------------
 *
 * cluster_hang_resolve.c
 *	  pgrac Hang Manager disposition (spec-5.12) — runtime layer.
 *
 *	  This is the remediation half of the Hang Manager.  It runs inside the
 *	  DIAG aux process, immediately after spec-5.11's long-wait sampling tick
 *	  (same process, same loop -> no cross-process publish/read window), and
 *	  for samples that are provably actionable (COMPLETE && !deadlock) it
 *	  selects the root blocker and, in enforce mode, cancels then terminates
 *	  it.  The pure decisions (score, gate verdict, confirmation machine,
 *	  tier->signal map, root ascent) live in cluster_hang_resolve_policy.c;
 *	  this file gathers the facts (pgstat + lock snapshots + the 5.8 WFG
 *	  probe), drives the fail-CLOSED gate with an at-signal-time ABA
 *	  re-validate, and sends the signal.
 *
 *	  fail-CLOSED: a victim is signalled only when every gate passes and its
 *	  identity is unchanged at the instant of signalling.  When anything is
 *	  uncertain we leave the hang for the finite-timeout backstop — under-
 *	  acting is the safe direction (mirror image of spec-5.11's fail-OPEN
 *	  diagnostics).  There is NEVER an internal SIGKILL (that crash-restarts
 *	  the whole instance); the strongest action is SIGTERM, and node-level
 *	  force is the external fencer plane (AD-013), forward to a future spec.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hang_resolve.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.12-hang-manager-disposition.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

/*
 * The two SQL functions are registered in pg_proc.dat unconditionally, so
 * fmgrtab must be able to link them in BOTH build modes (mirrors spec-5.11's
 * pg_cluster_hang_dump).  The real bodies + the disposition machinery live
 * under USE_PGRAC_CLUSTER; the --disable-cluster stubs are at the bottom.
 */
PG_FUNCTION_INFO_V1(pg_cluster_hang_victims);
PG_FUNCTION_INFO_V1(pg_cluster_hang_resolve);

#ifdef USE_PGRAC_CLUSTER

#include <signal.h>

#include "access/xlog.h"		 /* RecoveryInProgress */
#include "catalog/pg_authid_d.h" /* ROLE_PG_READ_ALL_STATS */
#include "funcapi.h"
#include "storage/lock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/timestamp.h"

#include "cluster/cluster_diag.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hang.h"
#include "cluster/cluster_hang_resolve.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lmd_wait_state.h"

/*
 * HangCandidate — one root-blocker candidate, gathered read-only from the
 * fresh pgstat + lock snapshots (no DIAG lock held during gathering).
 */
typedef struct HangCandidate {
	ClusterHangVictim v; /* pid / backendId / xid / ages / counts / score / skip */
	bool in_wfg;		 /* representative waiter is in the 5.8 WFG (over-exclude) */
	bool valid;			 /* a live backend was found for this pid */
} HangCandidate;


/* ============================================================
 * Lock-snapshot helpers
 * ============================================================ */

/* Two LOCKTAGs identify the same lockable object iff they are byte-equal. */
static inline bool
hang_locktag_eq(const LOCKTAG *a, const LOCKTAG *b)
{
	return memcmp(a, b, sizeof(LOCKTAG)) == 0;
}

/* True iff any granted mode in holdMask conflicts with waitLockMode. */
static bool
hang_holdmask_conflicts(LOCKMASK holdMask, LOCKMODE waitLockMode)
{
	int m;

	for (m = 1; m < MAX_LOCKMODES; m++) {
		if ((holdMask & LOCKBIT_ON(m)) && DoLockModesConflict(m, waitLockMode))
			return true;
	}
	return false;
}

/*
 * hang_victim_lock_facts -- count granted locks the victim holds and decide
 * whether it is actually blocking a waiter (G-lock-holder).
 *
 *	is_lock_blocker is true when the victim holds a granted lock on some
 *	locktag that another backend is waiting on with a conflicting mode.  This
 *	is identity-by-blocking, not identity-by-xid: an idle-in-tx
 *	"BEGIN; LOCK TABLE t;" holder with NO xid is correctly recognised as a
 *	blocker (spec v0.3 P1#1).  n_locks_held proxies rollback cost.
 */
static void
hang_victim_lock_facts(const LockData *lockData, int victim_pid, int *n_locks_held,
					   bool *is_lock_blocker)
{
	int held = 0;
	bool blocker = false;
	int i;

	for (i = 0; i < lockData->nelements; i++) {
		const LockInstanceData *li = &lockData->locks[i];
		int j;

		if (li->pid != victim_pid || li->holdMask == 0)
			continue;
		held++;

		for (j = 0; j < lockData->nelements; j++) {
			const LockInstanceData *w = &lockData->locks[j];

			if (j == i || w->pid == victim_pid || w->waitLockMode == NoLock)
				continue;
			if (!hang_locktag_eq(&li->locktag, &w->locktag))
				continue;
			if (hang_holdmask_conflicts(li->holdMask, w->waitLockMode)) {
				blocker = true;
				break;
			}
		}
	}

	*n_locks_held = held;
	*is_lock_blocker = blocker;
}


/* ============================================================
 * pgstat-snapshot helpers
 * ============================================================ */

/* Find the local pgstat beentry for a pid in the current snapshot, or NULL. */
static LocalPgBackendStatus *
hang_local_beentry_by_pid(int pid)
{
	int n = pgstat_fetch_stat_numbackends();
	int i;

	for (i = 1; i <= n; i++) {
		LocalPgBackendStatus *lb = pgstat_get_local_beentry_by_index(i);

		if (lb != NULL && lb->backendStatus.st_procpid == pid)
			return lb;
	}
	return NULL;
}


/* ============================================================
 * 5.8 WFG probe (over-exclude; v1 defensive / forward-ready)
 * ============================================================ */

bool
cluster_lmd_pid_in_wfg(int procno)
{
	PGPROC *proc;
	ClusterLmdWaitStateSnapshot ws;
	ClusterLmdVertex vtx;

	if (procno < 0)
		return false;
	proc = GetPGProcByNumber(procno);
	if (proc == NULL)
		return false;

	/*
	 * Only a backend with a published cluster (GES/TX) wait-state can be in
	 * the 5.8 WFG.  A pure local-lock waiter has no cluster wait-state, so it
	 * returns false here and is covered by the PG deadlock.c timing backstop
	 * instead (spec §0.3 / §3.2 G-over-exclude).
	 */
	if (!cluster_lmd_wait_state_read(&proc->cluster_lmd_wait, &ws) || !ws.active)
		return false;

	memset(&vtx, 0, sizeof(vtx));
	vtx.node_id = cluster_node_id;
	vtx.procno = (uint32)procno;
	vtx.cluster_epoch = ws.cluster_epoch;
	vtx.request_id = ws.request_id;
	vtx.xid = ws.xid;
	vtx.wait_seq = ws.wait_seq;
	return cluster_lmd_graph_has_waiter(&vtx);
}


/* ============================================================
 * Signal helper (D3) — cluster-internal pg_signal_backend equivalent
 * ============================================================ */

/*
 * cluster_hang_signal_victim -- send SIGINT/SIGTERM to a hang victim.
 *
 *	PG's pg_signal_backend() is file-static and applies SQL-role checks the
 *	trusted DIAG actor must not go through (its authority comes from the hang
 *	gate, not a SQL role).  We re-validate the target ourselves: it must be a
 *	live regular client backend (B_BACKEND) — rejecting postmaster / aux /
 *	walsender / bgworker / DIAG itself — before kill().  sig must be SIGINT or
 *	SIGTERM; SIGKILL is never accepted (spec §1.4 example 2).
 */
bool
cluster_hang_signal_victim(int pid, int sig)
{
	PGPROC *proc;
	LocalPgBackendStatus *lb;

	Assert(sig == SIGINT || sig == SIGTERM);
	if (sig != SIGINT && sig != SIGTERM)
		return false; /* default-deny: never any other signal */
	if (pid <= 0 || pid == MyProcPid)
		return false;

	/* must still be a live backend */
	proc = BackendPidGetProc(pid);
	if (proc == NULL)
		return false;

	/* must be a regular client backend (PGPROC has no backendType; read it
	 * from the pgstat beentry, the same accessor spec-5.11 uses) */
	lb = hang_local_beentry_by_pid(pid);
	if (lb == NULL || lb->backendStatus.st_backendType != B_BACKEND)
		return false;

	return kill(pid, sig) == 0;
}


/* ============================================================
 * Candidate gathering (read-only; no DIAG lock held)
 * ============================================================ */

/*
 * hang_gather_candidate -- fill a HangCandidate for a root-blocker pid from
 * the fresh pgstat + lock snapshots.  n_blocked / wait_age_us are supplied by
 * the caller (aggregated from the actionable waiters this root blocks).
 */
static void
hang_gather_candidate(HangCandidate *c, int victim_pid, int n_blocked, int64 wait_age_us,
					  const LockData *lockData, TimestampTz now)
{
	PGPROC *proc;
	LocalPgBackendStatus *lb;
	int backend_type = B_BACKEND;
	bool is_lock_blocker = false;
	int n_locks = 0;

	memset(c, 0, sizeof(*c));
	c->v.pid = victim_pid;
	c->v.n_blocked = n_blocked;
	c->v.wait_age_us = wait_age_us;
	c->valid = false;

	proc = BackendPidGetProc(victim_pid);
	if (proc == NULL)
		return; /* root vanished -> not a live candidate */

	lb = hang_local_beentry_by_pid(victim_pid);
	if (lb != NULL) {
		PgBackendStatus *be = &lb->backendStatus;

		backend_type = be->st_backendType;
		c->v.backendId = lb->backend_id;
		c->v.xid = lb->backend_xid;
		if (be->st_xact_start_timestamp != 0)
			c->v.xact_age_us = now - be->st_xact_start_timestamp;
	}

	hang_victim_lock_facts(lockData, victim_pid, &n_locks, &is_lock_blocker);
	c->v.n_locks_held = n_locks;

	/*
	 * HARD-skip + lock-holder classification.  is_recovery uses the
	 * instance-wide flag (a standby's regular backends are read-only and must
	 * never be disposed).  2PC-prepared has no live backend to signal, so a
	 * candidate with a live PGPROC is not a prepared orphan (is_prepared_2pc
	 * stays false); operator-critical flagging has no PG signal in v1
	 * (is_critical stays false) — both are honest v1 scope (spec §3.2 notes).
	 */
	c->v.skip = cluster_hang_victim_skip_reason(backend_type, RecoveryInProgress(), false, false,
												is_lock_blocker);
	c->v.score
		= cluster_hang_victim_score(&c->v, cluster_hang_victim_w_age,
									cluster_hang_victim_w_rollback, cluster_hang_victim_w_blockers);
	c->valid = true;
}

/*
 * hang_collect_candidates -- walk a 5.11 store snapshot, dedup to root
 * blockers, and gather facts for each.  Read-only (no DIAG lock).  Returns
 * the candidate count; *n_non_actionable / *n_over_excluded report the
 * waiters dropped by G-actionable / G-over-exclude so the caller can bump the
 * cumulative counters.  Used by both the periodic loop and the advisory SRF.
 */
static int
hang_collect_candidates(const ClusterHangSampleStore *snap, const LockData *lockData,
						TimestampTz now, HangCandidate *out, int max_out, uint64 *n_non_actionable,
						uint64 *n_over_excluded)
{
	int n_cand = 0;
	int i;

	*n_non_actionable = 0;
	*n_over_excluded = 0;

	for (i = 0; i < snap->n_samples; i++) {
		const ClusterHangSampleSlot *slot = &snap->slots[i];
		PGPROC *wproc;
		bool trunc = false;
		int root_pid;
		int k;

		if (!cluster_hang_sample_actionable(slot)) {
			(*n_non_actionable)++;
			continue;
		}

		/* G-over-exclude on the WAITER: a WFG waiter is handled by 5.8/5.9 */
		wproc = BackendPidGetProc(slot->pid);
		if (wproc != NULL && cluster_lmd_pid_in_wfg(wproc->pgprocno)) {
			(*n_over_excluded)++;
			continue;
		}

		root_pid = cluster_hang_root_blocker_pid(snap, i, cluster_hang_max_chain_depth, &trunc);
		if (root_pid < 0)
			continue;

		/* dedup: a root blocking several waiters appears once, n_blocked aggregates */
		for (k = 0; k < n_cand; k++) {
			if (out[k].v.pid == root_pid) {
				out[k].v.n_blocked++;
				if (slot->duration_us > out[k].v.wait_age_us)
					out[k].v.wait_age_us = slot->duration_us;
				/* keep the score consistent with the updated root-ness */
				out[k].v.score = cluster_hang_victim_score(&out[k].v, cluster_hang_victim_w_age,
														   cluster_hang_victim_w_rollback,
														   cluster_hang_victim_w_blockers);
				break;
			}
		}
		if (k < n_cand)
			continue;

		if (n_cand >= max_out)
			continue; /* bounded; excess roots dropped (rare) */

		hang_gather_candidate(&out[n_cand], root_pid, 1, slot->duration_us, lockData, now);
		if (out[n_cand].valid)
			n_cand++;
	}

	return n_cand;
}


/* ============================================================
 * ABA re-validate (G-ABA) — at the instant of signalling
 * ============================================================ */

/*
 * hang_revalidate_victim -- re-confirm, against a FRESH snapshot, that the
 * victim is still the same identity and still a lock blocker.  Guards the
 * tiny window between candidate gathering and the actual signal: the victim
 * may have released its lock / exited / had its pid reused.  fail-CLOSED:
 * any mismatch returns false and the caller never signals (spec §3.2 G-ABA).
 */
static bool
hang_revalidate_victim(int pid, int backendId)
{
	LockData *lockData;
	LocalPgBackendStatus *lb;
	int n_locks = 0;
	bool is_lock_blocker = false;

	if (BackendPidGetProc(pid) == NULL)
		return false;

	pgstat_clear_backend_activity_snapshot();
	lb = hang_local_beentry_by_pid(pid);
	if (lb == NULL || lb->backend_id != backendId || lb->backendStatus.st_backendType != B_BACKEND)
		return false; /* identity drifted / pid reused */

	lockData = GetLockStatusData();
	hang_victim_lock_facts(lockData, pid, &n_locks, &is_lock_blocker);
	return is_lock_blocker;
}


/* ============================================================
 * Periodic disposition loop body (D1)
 * ============================================================ */

/* one selected victim carried from the locked decision phase to the
 * unlocked signal phase */
typedef struct HangSelected {
	int pid;
	int backendId;
	ClusterHangActionTier tier; /* the tier decided this round (never NONE) */
} HangSelected;

/* throttle the honest-degrade / no-safe-victim operator alert to LOG-once */
static bool hang_degrade_alerted = false;

static void
hang_resolve_round(void)
{
	ClusterDiagSharedState *diag = cluster_diag_get_shared_state();
	ClusterHangSampleStore snap;
	LockData *lockData;
	HangCandidate cands[CLUSTER_HANG_MAX_SAMPLES];
	HangSelected selected[CLUSTER_HANG_MAX_SAMPLES];
	int n_cand;
	int n_sel = 0;
	int pass_idx[CLUSTER_HANG_MAX_SAMPLES];
	int n_pass = 0;
	uint64 n_non_actionable = 0;
	uint64 n_over_excluded = 0;
	bool any_hard_skip = false;
	bool enforce = cluster_hang_mode_dispatches(cluster_hang_resolution_mode);
	TimestampTz now = GetCurrentTimestamp();
	int i;

	if (diag == NULL)
		return;

	/* read-only snapshots first (each takes its own lock; no DIAG lock held) */
	cluster_hang_store_snapshot(&diag->hang_store, &diag->lwlock, &snap);
	if (snap.n_samples == 0) {
		/* nothing to do, but still account the evaluation + clear stale confirm
		 * state so a vanished victim's streak does not linger */
		LWLockAcquire(&diag->lwlock, LW_EXCLUSIVE);
		diag->hang_resolve_counters.resolve_evaluations++;
		cluster_hang_confirm_begin_round(&diag->hang_confirm_map);
		cluster_hang_confirm_end_round(&diag->hang_confirm_map);
		LWLockRelease(&diag->lwlock);
		hang_degrade_alerted = false;
		return;
	}

	pgstat_clear_backend_activity_snapshot();
	lockData = GetLockStatusData();
	n_cand = hang_collect_candidates(&snap, lockData, now, cands, CLUSTER_HANG_MAX_SAMPLES,
									 &n_non_actionable, &n_over_excluded);

	/* decision phase under the DIAG lock (confirm map + counters are shared) */
	LWLockAcquire(&diag->lwlock, LW_EXCLUSIVE);
	{
		ClusterHangResolveCounters *ctr = &diag->hang_resolve_counters;
		ClusterHangConfirmMap *map = &diag->hang_confirm_map;

		ctr->resolve_evaluations++;
		ctr->non_actionable_skipped += n_non_actionable;
		ctr->over_excluded += n_over_excluded;

		cluster_hang_confirm_begin_round(map);

		for (i = 0; i < n_cand; i++) {
			HangCandidate *c = &cands[i];
			ClusterHangConfirmEntry *e = NULL;
			bool confirmed = false;
			ClusterHangGateResult gate;

			if (c->v.skip == HANG_VICTIM_OK) {
				e = cluster_hang_confirm_touch(map, c->v.pid, c->v.backendId);
				confirmed = cluster_hang_confirm_ready(e, cluster_hang_resolution_confirm_rounds);
			}

			gate = cluster_hang_gate_decide(true, c->in_wfg, c->v.skip, confirmed);
			if (gate != HANG_GATE_PASS) {
				if (gate == HANG_GATE_HARD_SKIP)
					any_hard_skip = true;
				cluster_hang_counter_note_gate_reject(ctr, gate);
				continue;
			}
			pass_idx[n_pass++] = i;
		}

		/* resolved_confirmed: an identity we previously acted on is gone this
		 * round (not touched) -> the disposition worked */
		for (i = 0; i < CLUSTER_HANG_CONFIRM_MAX; i++) {
			ClusterHangConfirmEntry *e = &map->entries[i];

			if (e->in_use && !e->seen_this_round && e->last_action_tier != HANG_ACTION_NONE)
				ctr->resolved_confirmed++;
		}
		cluster_hang_confirm_end_round(map);

		/* deterministic order: best victim first, then rate-limit per round */
		if (n_pass > 1) {
			int a;

			for (a = 0; a < n_pass - 1; a++) {
				int b;

				for (b = a + 1; b < n_pass; b++) {
					if (cluster_hang_victim_score_cmp(&cands[pass_idx[b]].v, &cands[pass_idx[a]].v)
						< 0) {
						int t = pass_idx[a];

						pass_idx[a] = pass_idx[b];
						pass_idx[b] = t;
					}
				}
			}
		}

		for (i = 0; i < n_pass && i < cluster_hang_resolution_max_per_round; i++) {
			HangCandidate *c = &cands[pass_idx[i]];

			if (!enforce) {
				/* advisory: record the recommendation, never signal */
				ctr->advisory_recommendations++;
				ereport(LOG, (errmsg("cluster hang manager (advisory): would dispose pid %d "
									 "(blocks %d waiter(s), score %.3f); set "
									 "cluster.hang_resolution_mode=enforce to act",
									 c->v.pid, c->v.n_blocked, c->v.score)));
				continue;
			}

			/* enforce: decide the tier for this round from the confirm entry */
			{
				ClusterHangConfirmEntry *e
					= cluster_hang_confirm_touch(map, c->v.pid, c->v.backendId);
				ClusterHangActionTier tier = cluster_hang_confirm_decide_tier(
					e, now, cluster_hang_resolution_soft_timeout_ms);

				if (tier == HANG_ACTION_NONE)
					continue; /* still inside the anti-thrash grace window */
				selected[n_sel].pid = c->v.pid;
				selected[n_sel].backendId = c->v.backendId;
				selected[n_sel].tier = tier;
				n_sel++;
			}
		}
	}
	LWLockRelease(&diag->lwlock);

	/* honest-degrade: actionable hangs existed but every candidate is a
	 * never-kill backend -> alert once, rely on the finite-timeout backstop */
	if (enforce && n_pass == 0 && any_hard_skip) {
		LWLockAcquire(&diag->lwlock, LW_EXCLUSIVE);
		diag->hang_resolve_counters.no_safe_victim++;
		LWLockRelease(&diag->lwlock);
		if (!hang_degrade_alerted) {
			ereport(LOG, (errmsg("cluster hang manager: actionable hang(s) detected but all "
								 "root blockers are protected (never-kill) backends; not "
								 "forcing — relying on finite-timeout backstop / operator "
								 "or external fencer (AD-013)")));
			hang_degrade_alerted = true;
		}
	} else if (n_pass > 0)
		hang_degrade_alerted = false;

	if (n_sel == 0)
		return;

	/* signal phase: no DIAG lock held; ABA re-validate at the instant of action */
	for (i = 0; i < n_sel; i++) {
		HangSelected *s = &selected[i];
		bool acted = false;
		bool aba_ok = hang_revalidate_victim(s->pid, s->backendId);

		LWLockAcquire(&diag->lwlock, LW_EXCLUSIVE);
		{
			ClusterHangResolveCounters *ctr = &diag->hang_resolve_counters;
			ClusterHangConfirmEntry *e
				= cluster_hang_confirm_touch(&diag->hang_confirm_map, s->pid, s->backendId);

			if (!aba_ok) {
				ctr->aba_revalidate_failed++;
			} else if (s->tier == HANG_ACTION_DEGRADE) {
				ctr->degraded_to_timeout++;
				ctr->last_victim_pid = s->pid;
				ctr->last_action = (uint8)HANG_ACTION_DEGRADE;
				cluster_hang_confirm_record_action(e, HANG_ACTION_DEGRADE, now);
				acted = true; /* degrade is "acted" for alerting purposes */
			} else {
				int sig = cluster_hang_tier_signal(s->tier);

				if (sig != 0 && cluster_hang_signal_victim(s->pid, sig)) {
					if (s->tier == HANG_ACTION_CANCEL)
						ctr->soft_cancels_issued++;
					else if (s->tier == HANG_ACTION_TERMINATE)
						ctr->terminates_issued++;
					ctr->victims_selected++;
					ctr->last_victim_pid = s->pid;
					ctr->last_action = (uint8)s->tier;
					cluster_hang_confirm_record_action(e, s->tier, now);
					acted = true;
				} else {
					/* signal helper rejected the target / kill failed: record
					 * the failure and do NOT escalate to a stronger measure */
					ctr->resolution_failed++;
				}
			}
		}
		LWLockRelease(&diag->lwlock);

		if (acted && s->tier == HANG_ACTION_DEGRADE) {
			if (!hang_degrade_alerted) {
				ereport(LOG, (errmsg("cluster hang manager: pid %d still hanging after "
									 "terminate; honest-degrade (no SIGKILL) — escalate to "
									 "operator / external fencer (AD-013)",
									 s->pid)));
				hang_degrade_alerted = true;
			}
		} else if (acted)
			ereport(LOG, (errmsg("cluster hang manager: %s pid %d (hang disposition)",
								 cluster_hang_action_tier_str(s->tier), s->pid)));
	}
}

void
cluster_hang_resolve_once(void)
{
	/* fail-CLOSED + never-throw: a disposition fault must never crash DIAG */
	PG_TRY();
	{
		hang_resolve_round();
	}
	PG_CATCH();
	{
		ClusterDiagSharedState *diag = cluster_diag_get_shared_state();

		if (diag != NULL) {
			LWLockAcquire(&diag->lwlock, LW_EXCLUSIVE);
			diag->hang_resolve_counters.resolution_failed++;
			LWLockRelease(&diag->lwlock);
		}
		FlushErrorState();
	}
	PG_END_TRY();
}


/* ============================================================
 * Manual single-victim disposition (D7 backend of pg_cluster_hang_resolve)
 * ============================================================ */

/*
 * cluster_hang_resolve_pid -- validate that pid is currently an actionable
 * root blocker and, if so, terminate it.  Applies the same fail-CLOSED gates
 * as the periodic loop EXCEPT G-confirm (the operator's explicit call is the
 * confirmation) and goes straight to SIGTERM (the operator wants it gone; a
 * cancel does nothing for the common idle-in-tx blocker).  Returns true iff a
 * terminate was issued.  Callers (the SQL wrapper) gate on superuser.
 */
bool
cluster_hang_resolve_pid(int pid)
{
	ClusterDiagSharedState *diag = cluster_diag_get_shared_state();
	ClusterHangSampleStore snap;
	LockData *lockData;
	TimestampTz now = GetCurrentTimestamp();
	bool is_root_of_actionable = false;
	HangCandidate c;
	int i;

	if (diag == NULL || pid <= 0)
		return false;

	cluster_hang_store_snapshot(&diag->hang_store, &diag->lwlock, &snap);
	pgstat_clear_backend_activity_snapshot();
	lockData = GetLockStatusData();

	/* pid must be the root blocker of at least one actionable, non-WFG waiter */
	for (i = 0; i < snap.n_samples; i++) {
		const ClusterHangSampleSlot *slot = &snap.slots[i];
		PGPROC *wproc;
		bool trunc = false;

		if (!cluster_hang_sample_actionable(slot))
			continue;
		wproc = BackendPidGetProc(slot->pid);
		if (wproc != NULL && cluster_lmd_pid_in_wfg(wproc->pgprocno))
			continue;
		if (cluster_hang_root_blocker_pid(&snap, i, cluster_hang_max_chain_depth, &trunc) == pid) {
			is_root_of_actionable = true;
			break;
		}
	}
	if (!is_root_of_actionable)
		return false;

	hang_gather_candidate(&c, pid, 1, 0, lockData, now);
	if (!c.valid || c.v.skip != HANG_VICTIM_OK)
		return false; /* not a live regular backend / HARD-skip / not a lock holder */

	/* G-ABA at the instant of signalling, then terminate */
	if (!hang_revalidate_victim(pid, c.v.backendId))
		return false;
	if (!cluster_hang_signal_victim(pid, SIGTERM))
		return false;

	LWLockAcquire(&diag->lwlock, LW_EXCLUSIVE);
	diag->hang_resolve_counters.terminates_issued++;
	diag->hang_resolve_counters.victims_selected++;
	diag->hang_resolve_counters.last_victim_pid = pid;
	diag->hang_resolve_counters.last_action = (uint8)HANG_ACTION_TERMINATE;
	LWLockRelease(&diag->lwlock);
	ereport(LOG, (errmsg("cluster hang manager: terminate pid %d (manual pg_cluster_hang_resolve)",
						 pid)));
	return true;
}

/*
 * cluster_hang_resolve_get_counters -- consistent copy of the disposition
 * counters (+ last-action) for dump_hang (D8).  Zeroed when DIAG is not
 * attached (e.g. cluster disabled).
 */
void
cluster_hang_resolve_get_counters(ClusterHangResolveCounters *out)
{
	ClusterDiagSharedState *diag = cluster_diag_get_shared_state();

	if (diag == NULL) {
		memset(out, 0, sizeof(*out));
		return;
	}
	LWLockAcquire(&diag->lwlock, LW_SHARED);
	*out = diag->hang_resolve_counters;
	LWLockRelease(&diag->lwlock);
}


/* ============================================================
 * SQL entry points (D7)
 * ============================================================ */

/*
 * pg_cluster_hang_victims() -- advisory view of the current actionable root
 * blockers + scores + recommended action.  Carries other backends' pid/xid,
 * so it is gated like pg_stat_activity: pg_read_all_stats members / superuser
 * see every row; an unprivileged user sees only rows for victims owned by
 * their own role (spec §2.5 / Q9 / L9).
 */
Datum
pg_cluster_hang_victims(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo;
	ClusterDiagSharedState *diag = cluster_diag_get_shared_state();
	ClusterHangSampleStore snap;
	LockData *lockData;
	HangCandidate cands[CLUSTER_HANG_MAX_SAMPLES];
	int n_cand = 0;
	uint64 dummy_a = 0;
	uint64 dummy_o = 0;
	bool privileged = has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS);
	TimestampTz now = GetCurrentTimestamp();
	int i;

	InitMaterializedSRF(fcinfo, 0);
	rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;

	if (diag == NULL)
		return (Datum)0;

	cluster_hang_store_snapshot(&diag->hang_store, &diag->lwlock, &snap);
	pgstat_clear_backend_activity_snapshot();
	lockData = GetLockStatusData();
	n_cand = hang_collect_candidates(&snap, lockData, now, cands, CLUSTER_HANG_MAX_SAMPLES,
									 &dummy_a, &dummy_o);

	for (i = 0; i < n_cand; i++) {
		HangCandidate *c = &cands[i];
		Datum values[8];
		bool nulls[8];
		int col = 0;
		const char *skip_str;
		const char *rec;

		/* per-row visibility gate (mirror pg_stat_activity) */
		if (!privileged) {
			LocalPgBackendStatus *lb = hang_local_beentry_by_pid(c->v.pid);

			if (lb == NULL || lb->backendStatus.st_userid != GetUserId())
				continue;
		}

		switch (c->v.skip) {
		case HANG_VICTIM_OK:
			skip_str = "";
			break;
		case HANG_VICTIM_SKIP_SYSTEM:
			skip_str = "system";
			break;
		case HANG_VICTIM_SKIP_RECOVERY:
			skip_str = "recovery";
			break;
		case HANG_VICTIM_SKIP_2PC:
			skip_str = "2pc";
			break;
		case HANG_VICTIM_SKIP_CRITICAL:
			skip_str = "critical";
			break;
		case HANG_VICTIM_SKIP_NOT_LOCK_HOLDER:
			skip_str = "not_lock_holder";
			break;
		default:
			skip_str = "(unknown)";
			break;
		}

		if (c->v.skip != HANG_VICTIM_OK)
			rec = "skip";
		else if (c->in_wfg)
			rec = "skip:deadlock-managed";
		else
			rec = "cancel-then-terminate";

		memset(nulls, 0, sizeof(nulls));
		values[col++] = Int32GetDatum(c->v.pid);
		values[col++] = Int32GetDatum(c->v.backendId);
		if (TransactionIdIsValid(c->v.xid))
			values[col++] = TransactionIdGetDatum(c->v.xid);
		else
			nulls[col++] = true;
		values[col++] = Int64GetDatum(c->v.wait_age_us / 1000); /* ms */
		values[col++] = Int32GetDatum(c->v.n_blocked);
		values[col++] = Float8GetDatum(c->v.score);
		values[col++] = CStringGetTextDatum(skip_str);
		values[col++] = CStringGetTextDatum(rec);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	return (Datum)0;
}

/*
 * pg_cluster_hang_resolve(pid) -- manually dispose of one victim (superuser
 * only), subject to the same fail-CLOSED gate as the periodic loop.  Returns
 * true iff a terminate was issued; false if the pid is not currently an
 * actionable root blocker (no force).
 */
Datum
pg_cluster_hang_resolve(PG_FUNCTION_ARGS)
{
	int pid = PG_GETARG_INT32(0);

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to resolve a cluster hang victim")));

	PG_RETURN_BOOL(cluster_hang_resolve_pid(pid));
}

#else /* !USE_PGRAC_CLUSTER */

/*
 * --disable-cluster stubs.  The pg_proc.dat entries are unconditional so
 * fmgrtab links in both modes; the disposition machinery only exists in
 * --enable-cluster builds.
 */
Datum
pg_cluster_hang_victims(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_hang_victims requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

Datum
pg_cluster_hang_resolve(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("pg_cluster_hang_resolve requires a --enable-cluster build")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
