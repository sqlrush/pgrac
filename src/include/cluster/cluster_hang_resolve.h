/*-------------------------------------------------------------------------
 *
 * cluster_hang_resolve.h
 *	  pgrac Hang Manager disposition (spec-5.12) — local (same-node)
 *	  remediation of non-deadlock hangs: victim selection, tiered
 *	  cancel/terminate, a cross-round confirmation/hysteresis state
 *	  machine, and a fail-CLOSED disposition gate.
 *
 *	  This is the remediation half of the Hang Manager.  spec-5.11
 *	  (cluster_hang.{c,h}) only DIAGNOSES — it samples long-waiting
 *	  backends, tags each with a completeness quality, and exposes them;
 *	  it never cancels, terminates, or selects a victim.  spec-5.12
 *	  CONSUMES the spec-5.11 sample store and, for samples that are
 *	  provably actionable (cluster_hang_sample_actionable(): COMPLETE &&
 *	  !in_confirmed_deadlock), selects the root blocker and disposes of it.
 *
 *	  Discipline is the mirror image of spec-5.11.  The diagnostic side is
 *	  fail-OPEN (cannot prove → tag INCOMPLETE and keep going).  The
 *	  disposition side is fail-CLOSED (cannot prove → never act): a victim
 *	  is cancelled/terminated only when every gate passes (actionable,
 *	  not in the 5.8 WFG, not a HARD-skip backend, currently granted-holding
 *	  the conflicting lock, confirmed across rounds, and identity unchanged
 *	  at the moment of signalling).  When in doubt we leave the hang for the
 *	  finite-timeout backstop — under-acting is the safe direction.
 *
 *	  There is NEVER an internal SIGKILL: SIGKILL of a backend makes the
 *	  postmaster crash-restart the whole instance (RAC reconfiguration).
 *	  The strongest action is SIGTERM (session terminate); node-level force
 *	  (kill -9 / fence / reboot) is the external watchdog/fencer plane
 *	  (AD-013), forward to a future spec.
 *
 *	  Layering mirrors spec-5.11:
 *	    - cluster_hang_resolve_policy.c : pure decision layer (score,
 *	      score_cmp, skip_reason, fail-CLOSED gate, tier→signal map, mode
 *	      predicates, root-blocker ascent, confirmation/hysteresis state
 *	      machine).  No PostgreSQL runtime dependency, so it is exercised
 *	      directly by cluster_unit (test_cluster_hang_resolve).
 *	    - cluster_hang_resolve.c : runtime layer (DIAG tick hook, pgstat +
 *	      lock-snapshot gathering, the cluster_hang_signal_victim helper,
 *	      the tiered dispatcher, the 5.8 WFG probe wrapper, and the two SQL
 *	      entry points).  Exercised by cluster_tap t/305.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_hang_resolve.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-5.12-hang-manager-disposition.md
 *	  Leaf header: depends only on base postgres types + timestamp + the
 *	  spec-5.11 cluster_hang.h sample store, so the disposition counters and
 *	  the confirmation map can be embedded into ClusterDiagSharedState
 *	  (cluster_diag.h) without adding a shmem region.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_HANG_RESOLVE_H
#define CLUSTER_HANG_RESOLVE_H

#include "datatype/timestamp.h"
#include "cluster/cluster_hang.h"

/*
 * Upper bound on tracked victim identities in the confirmation/hysteresis
 * map.  One entry per candidate root blocker; bounded by the same constant
 * as the sample store so the map is a fixed-size array embeddable into the
 * DIAG shmem region (no new region / tranche, mirrors spec-5.11 Q13-A).
 */
#define CLUSTER_HANG_CONFIRM_MAX CLUSTER_HANG_MAX_SAMPLES


/*
 * ClusterHangResolveMode — cluster.hang_resolution_mode GUC.
 *
 *	OFF      : do not evaluate disposition (spec-5.11 diagnostics still run).
 *	ADVISORY : evaluate + record recommendations / counters / LOG, but never
 *	           actually cancel or terminate (dry-run).  Factory default.
 *	ENFORCE  : really dispose (cancel → terminate ladder).  Opt-in.
 */
typedef enum ClusterHangResolveMode {
	HANG_RESOLVE_OFF = 0,
	HANG_RESOLVE_ADVISORY,
	HANG_RESOLVE_ENFORCE
} ClusterHangResolveMode;

/*
 * ClusterHangVictimSkip — why a candidate root blocker is not eligible for
 * disposition.  The first four are the HARD-skip never-kill set (counted as
 * hard_skipped); NOT_LOCK_HOLDER means the candidate is not currently
 * granted-holding the conflicting lock (G-lock-holder failed / identity
 * drifted), so it is not a valid victim.
 */
typedef enum ClusterHangVictimSkip {
	HANG_VICTIM_OK = 0,
	HANG_VICTIM_SKIP_SYSTEM, /* not a regular client backend (aux / autovac / bgworker / walsender) */
	HANG_VICTIM_SKIP_RECOVERY,		 /* recovery / startup */
	HANG_VICTIM_SKIP_2PC,			 /* 2PC-prepared (no live backend to signal) */
	HANG_VICTIM_SKIP_CRITICAL,		 /* operator-flagged critical session */
	HANG_VICTIM_SKIP_NOT_LOCK_HOLDER /* not granted-holding the conflicting lock */
} ClusterHangVictimSkip;

/*
 * ClusterHangActionTier — disposition ladder.  CANCEL then TERMINATE then
 * honest-degrade.  There is deliberately no SIGKILL tier (see file header).
 */
typedef enum ClusterHangActionTier {
	HANG_ACTION_NONE = 0,
	HANG_ACTION_CANCEL,	   /* Tier-1: SIGINT — cancel the running query */
	HANG_ACTION_TERMINATE, /* Tier-2: SIGTERM — terminate the session */
	HANG_ACTION_DEGRADE	   /* Tier-3: honest-degrade (LOG-once + alert, no SIGKILL) */
} ClusterHangActionTier;

/*
 * ClusterHangGateResult — verdict of the fail-CLOSED disposition gate
 * (cluster_hang_gate_decide()).  PASS means every gate up to ABA passed;
 * anything else names the gate that rejected the candidate and maps to the
 * matching disposition counter.
 */
typedef enum ClusterHangGateResult {
	HANG_GATE_PASS = 0,
	HANG_GATE_NOT_ACTIONABLE,  /* G-actionable: not COMPLETE / in confirmed deadlock */
	HANG_GATE_OVER_EXCLUDED,   /* G-over-exclude: in the 5.8 WFG (v1 defensive-unreachable) */
	HANG_GATE_HARD_SKIP,	   /* G-hard-skip: never-kill backend class */
	HANG_GATE_NOT_LOCK_HOLDER, /* G-lock-holder: not granted-holding the conflicting lock */
	HANG_GATE_NOT_CONFIRMED	   /* G-confirm: not yet confirmed across confirm_rounds */
} ClusterHangGateResult;

/*
 * ClusterHangVictim — one candidate root blocker plus the facts feeding the
 * victim score.  POD; built per-round by the runtime layer from the pgstat +
 * lock snapshots and scored by the pure layer.
 */
typedef struct ClusterHangVictim {
	int pid;
	int backendId;
	TransactionId xid; /* pgstat backend_xid: rollback-cost signal, NOT a gate */
	int64 wait_age_us; /* longest actionable waiter's COMPLETE wait this victim blocks */
	int64 xact_age_us; /* xact_start -> now (age factor) */
	int n_locks_held;  /* rollback-cost proxy */
	int n_blocked;	   /* number of actionable waiters this victim blocks (root-ness) */
	double score;
	ClusterHangVictimSkip skip;
} ClusterHangVictim;

/*
 * ClusterHangResolveCounters — cumulative disposition observability counters
 * (D8).  Embedded in the DIAG shmem region next to the spec-5.11 counters;
 * all updates take the DIAG LWLock.
 */
typedef struct ClusterHangResolveCounters {
	uint64 resolve_evaluations;	   /* disposition evaluation rounds */
	uint64 victims_selected;	   /* victims chosen for disposition */
	uint64 soft_cancels_issued;	   /* Tier-1 cancel signals issued */
	uint64 terminates_issued;	   /* Tier-2 terminate signals issued */
	uint64 resolved_confirmed;	   /* victim confirmed gone / no longer long-wait */
	uint64 resolution_failed;	   /* signal helper rejected (no proc / bad target / kill failed) */
	uint64 hard_skipped;		   /* HARD-skip never-kill class hit */
	uint64 non_actionable_skipped; /* G-actionable rejected (remote/approx/in-deadlock) */
	uint64 over_excluded;		   /* COMPLETE but in 5.8 WFG (v1 unreachable, forward GES-class) */
	uint64 unprovable_root_skipped; /* truncated/cyclic root ascent -> skip (F3) */
	uint64 aba_revalidate_failed;	/* pre-signal re-validate: edge/identity drifted -> never act */
	uint64 not_confirmed_yet;		/* below confirm_rounds */
	uint64 no_safe_victim;			/* all candidates HARD-skip -> degrade */
	uint64 degraded_to_timeout;		/* Tier-3 honest-degrade */
	uint64 advisory_recommendations; /* advisory-mode recommendations recorded */
	/* last-action observability (not counters; written under the DIAG lock) */
	int last_victim_pid; /* pid of the most recently disposed victim, 0 = none */
	uint8 last_action;	 /* ClusterHangActionTier of the most recent disposition */
} ClusterHangResolveCounters;

/*
 * ClusterHangConfirmEntry — per-victim-identity cross-round state for the
 * confirmation/hysteresis + anti-thrash + tier-escalation machine (D5).
 */
typedef struct ClusterHangConfirmEntry {
	bool in_use;
	bool seen_this_round; /* set by touch(), cleared by begin_round() */
	int pid;
	int backendId;
	int consecutive_rounds;		/* consecutive rounds confirmed actionable long-wait */
	uint8 last_action_tier;		/* ClusterHangActionTier last applied to this identity */
	TimestampTz last_action_at; /* 0 = never acted; basis for the soft-timeout grace */
} ClusterHangConfirmEntry;

/*
 * ClusterHangConfirmMap — fixed-size bounded map of tracked identities.
 * Embedded into the DIAG shmem region; written only by DIAG under the lock.
 */
typedef struct ClusterHangConfirmMap {
	ClusterHangConfirmEntry entries[CLUSTER_HANG_CONFIRM_MAX];
} ClusterHangConfirmMap;


/* ============================================================
 * Pure decision layer (cluster_hang_resolve_policy.c) — no PG runtime.
 * ============================================================ */

/* victim score (higher = dispose first).  Weights are passed in (the GUC
 * values at runtime); priority/cpu are NOT factors — PG exposes no such
 * signal (spec §8 Q7). */
extern double cluster_hang_victim_score(const ClusterHangVictim *v, double w_age, double w_rollback,
										double w_blockers);

/* deterministic ordering: score DESC, tie -> wait_age DESC -> n_blocked DESC
 * -> pid ASC.  Uses the precomputed v->score.  qsort-compatible (descending,
 * so the highest-score victim sorts first). */
extern int cluster_hang_victim_score_cmp(const ClusterHangVictim *a, const ClusterHangVictim *b);

/* HARD-skip + lock-holder classification (judged locally, zero attr transfer). */
extern ClusterHangVictimSkip cluster_hang_victim_skip_reason(int backend_type, bool is_recovery,
															 bool is_prepared_2pc, bool is_critical,
															 bool is_lock_holder);

/* fail-CLOSED gate verdict (default-deny ordering, spec §3.2).  skip already
 * encodes HARD-skip and NOT_LOCK_HOLDER; in_wfg is the 5.8 WFG membership;
 * confirmed is the D5 confirm verdict. */
extern ClusterHangGateResult cluster_hang_gate_decide(bool actionable, bool in_wfg,
													  ClusterHangVictimSkip skip, bool confirmed);

/* tier -> OS signal.  CANCEL=SIGINT, TERMINATE=SIGTERM, everything else
 * (NONE / DEGRADE / unknown) = 0 (default-deny: an unknown tier never
 * "falls through" into a destructive signal, spec §3.2 L237).  Never SIGKILL. */
extern int cluster_hang_tier_signal(ClusterHangActionTier tier);

/* Bump the disposition counter matching a gate rejection (NOT_ACTIONABLE ->
 * non_actionable_skipped, OVER_EXCLUDED -> over_excluded, HARD_SKIP ->
 * hard_skipped, NOT_CONFIRMED -> not_confirmed_yet).  PASS and NOT_LOCK_HOLDER
 * have no dedicated counter and bump nothing.  Pure (caller holds the lock). */
extern void cluster_hang_counter_note_gate_reject(ClusterHangResolveCounters *c,
												  ClusterHangGateResult r);

/* mode predicates (spec §3.4). */
extern bool cluster_hang_mode_evaluates(ClusterHangResolveMode mode);  /* advisory or enforce */
extern bool cluster_hang_mode_dispatches(ClusterHangResolveMode mode); /* enforce only */

/* root-first ascent over a sample-store snapshot: from start_slot follow
 * blocker_pid while the blocker is itself a sampled long-waiter, stopping at
 * the first blocker that is NOT a sampled waiter (the idle-in-tx root holder)
 * and returning its pid.  *direct_waiter_pid (if non-NULL) returns the pid of
 * the sampled waiter the root directly blocks (the chain edge for G-ABA, -1 if
 * no root).  Cycle-guarded + depth-bounded; sets *truncated when a cycle or the
 * depth limit cut the walk short.  Returns -1 if the start slot has no blocker. */
extern int cluster_hang_root_blocker_pid(const ClusterHangSampleStore *store, int start_slot,
										 int max_depth, bool *truncated, int *direct_waiter_pid);

/* confirmation / hysteresis state machine (operates on a plain map; the
 * runtime points it at the embedded DIAG-region map under the lock). */
extern void cluster_hang_confirm_begin_round(ClusterHangConfirmMap *map);
extern ClusterHangConfirmEntry *cluster_hang_confirm_touch(ClusterHangConfirmMap *map, int pid,
														   int backendId);
extern void cluster_hang_confirm_end_round(ClusterHangConfirmMap *map);
extern bool cluster_hang_confirm_ready(const ClusterHangConfirmEntry *e, int confirm_rounds);
/* Which tier to apply this round given the last action + elapsed grace.  Pure:
 * NONE last -> CANCEL; CANCEL last -> TERMINATE once soft_timeout elapsed else
 * NONE (anti-thrash grace); TERMINATE last -> DEGRADE once elapsed else NONE;
 * DEGRADE last -> DEGRADE. */
extern ClusterHangActionTier cluster_hang_confirm_decide_tier(const ClusterHangConfirmEntry *e,
															  TimestampTz now, int soft_timeout_ms);
extern void cluster_hang_confirm_record_action(ClusterHangConfirmEntry *e,
											   ClusterHangActionTier tier, TimestampTz now);
/* Count identities that were previously acted on (last_action_tier != NONE) but
 * are no longer seen this round -> the disposition worked.  Must be called after
 * begin_round()/touch() and before end_round() (which evicts the unseen entries).
 * Pure; the caller adds the result to resolved_confirmed under the DIAG lock
 * (Hardening v1.1 F4: both the normal and the empty-sample round use this so a
 * successful terminate that clears all samples is still counted). */
extern int cluster_hang_confirm_count_resolved(const ClusterHangConfirmMap *map);


/* ============================================================
 * Runtime layer (cluster_hang_resolve.c).
 * ============================================================ */

/* One disposition evaluation round.  fail-CLOSED; never throws (internal
 * PG_TRY backstop).  Called from DiagMain right after cluster_hang_sample_once()
 * when cluster.hang_resolution_mode != off. */
extern void cluster_hang_resolve_once(void);

/*
 * Send a cancel/terminate signal to a hang victim.  Cluster-internal
 * equivalent of the file-static pg_signal_backend(): validates that pid is a
 * live regular client backend (B_BACKEND; rejects postmaster / aux /
 * walsender / bgworker / self) before kill().  Returns true on success.
 * sig must be SIGINT or SIGTERM (asserts; never SIGKILL).
 */
extern bool cluster_hang_signal_victim(int pid, int sig);

/* True iff a backend (by PGPROC procno) currently appears as a waiter in the
 * 5.8 wait-for graph.  Wraps vertex construction + cluster_lmd_graph_has_waiter;
 * a pure local-lock waiter with no cluster wait-state returns false (handled
 * by the PG deadlock.c timing backstop instead).  v1 defensive / forward-ready. */
extern bool cluster_lmd_pid_in_wfg(int procno);

/* Manual single-victim disposition entry shared by pg_cluster_hang_resolve():
 * applies the same fail-CLOSED gates and terminates the victim if it passes.
 * Returns true if a terminate was issued.  (The periodic dispatcher is static
 * to cluster_hang_resolve.c since it carries the per-round confirm context.) */
extern bool cluster_hang_resolve_pid(int pid);

/* SQL entry points (D7) are declared with PG_FUNCTION_INFO_V1 in
 * cluster_hang_resolve.c (standard fmgr pattern; no header extern needed). */

/* Copy the disposition counters (+ last-action) from the DIAG region under the
 * DIAG lock; zeroed when the region is not attached.  Used by dump_hang (D8). */
extern void cluster_hang_resolve_get_counters(ClusterHangResolveCounters *out);

/* string name for a disposition mode / tier / skip / gate result (dumps + logs). */
extern const char *cluster_hang_resolve_mode_str(int mode);
extern const char *cluster_hang_action_tier_str(int tier);

#endif /* CLUSTER_HANG_RESOLVE_H */
