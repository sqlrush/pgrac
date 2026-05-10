/*-------------------------------------------------------------------------
 *
 * cluster_fence.h
 *	  pgrac Fence-lite — spec-2.28 Sprint A Step 1.
 *
 *	  Internal-only fence-lite: ProcSignal-driven freeze/thaw broadcast
 *	  for in-flight transaction abort + postmaster orderly self-shutdown
 *	  on persistent quorum loss.  Consumes ClusterQvotecShmem.quorum_
 *	  state (spec-2.6) via LMON tick;does NOT introduce a new aux
 *	  process (Q3 = A LMON-mediated; AD-013 reject 7th aux process).
 *
 *	  Three user-imposed invariants (spec §3.0):
 *
 *	    I1 Freeze immediate / self-shutdown delayed.  LMON broadcasts
 *	       PROCSIG_CLUSTER_FREEZE_WRITES the moment quorum_state goes
 *	       OK→LOST (no grace).  cluster.self_fence_grace_ms only
 *	       controls the postmaster's pmdie wait — it does NOT delay
 *	       the in-flight tx abort path.
 *
 *	    I2 Thaw informational only — does NOT bypass commit gate.
 *	       PROCSIG_CLUSTER_THAW_WRITES handler ONLY refreshes the
 *	       observability view (last_thaw_at_us) + DEBUG2 log.  It
 *	       does NOT clear ClusterFenceFreezePending (race-prone) and
 *	       does NOT change cluster_qvotec_in_quorum() return.  The
 *	       commit gate (spec-2.6 v0.14.1+ in xact.c) stays
 *	       authoritative for commit predicate.
 *
 *	    I3 spec-2.6 P1 prerequisite.  Sprint A Step 1 commit message
 *	       must reference linkdb v0.14.2-stage2.6 nightly run ID
 *	       25618433189 (Hardening v0.6 F1+F2 closed:quorum_state
 *	       post-write recompute + fast-restart ghost-detect dual
 *	       layer).  Without those fixes, freeze broadcast can fire
 *	       on stale quorum_state and abort healthy in-flight tx.
 *
 *	  Step 1 scope (this commit):
 *	    - Public API decl (init / broadcast_freeze / broadcast_thaw /
 *	      self_request / check_interrupts / postmaster_check)
 *	    - 4 PGC_POSTMASTER GUC extern declarations
 *	    - ClusterFenceFreezePending volatile sig_atomic_t flag
 *	      (per-backend, signal-safe write from cluster_signal.c
 *	      handler;reader is cluster_fence_check_interrupts under
 *	      ProcessInterrupts which already guards CritSectionCount > 0
 *	      per postgres.c:3226-3227 — v0.3 F1 amend, no manual CS
 *	      guard needed inside hook body)
 *	    - SRF callback decl for pg_cluster_fence_state view
 *	    - shmem_size / shmem_init / shmem_register
 *
 *	  Step 1 explicitly DEFERS:
 *	    - Real ProcSignal handler bodies (cluster_signal.c — Step 2 D3)
 *	    - Backend ProcessInterrupts hook (postgres.c — Step 2 D4)
 *	    - LMON quorum_state poll + broadcast (cluster_lmon.c — Step 3 D5)
 *	    - Postmaster orderly shutdown trigger via kill(MyProcPid,
 *	      SIGINT) (postmaster.c — Step 3 D6, v0.3 F4 amend per
 *	      "no `pmdie()` callable")
 *	    - 1 SQLSTATE + 1 wait event + view + counters + inject (Step 4)
 *	    - 098 TAP fence_freeze_writes_2node + 096 L4-L5 unblock (Step 5)
 *
 *	  Until Step 2-3 land, broadcast_freeze / broadcast_thaw / self_
 *	  request are stubs that update ClusterFenceShmem fields but do NOT
 *	  iterate ProcArray / send signals.  This lets cluster_unit T-fence-1
 *	  verify GUC default registration without pulling in PG runtime.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_fence.h
 *
 * NOTES
 *	  pgrac-original file.  Compiled only in --enable-cluster builds
 *	  (USE_PGRAC_CLUSTER guard); not referenced by pg_proc.dat in
 *	  Step 1 (SRF lands Step 4).
 *
 *	  Spec authority:  pgrac:specs/spec-2.28-fence-lite-self-fence-
 *	  procsignal-freeze-thaw.md (frozen v0.3 2026-05-10 Q1-Q8 user
 *	  approve + 3 invariant + 2 spec-bug pre-Sprint-A correction).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_FENCE_H
#define CLUSTER_FENCE_H

#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include <signal.h> /* sig_atomic_t */
#include "fmgr.h"	/* PG_FUNCTION_ARGS */


/* ============================================================
 * Per-backend volatile flag — set by PROCSIG_CLUSTER_FREEZE_WRITES
 * handler (signal-safe atomic write of 1 + SetLatch); read-and-cleared
 * by cluster_fence_check_interrupts when called from ProcessInterrupts.
 *
 * Per spec §3.0 Invariant I2: PROCSIG_CLUSTER_THAW_WRITES handler
 * does NOT clear this flag — the next ProcessInterrupts cycle is
 * authoritative.  Aborting in-flight tx unnecessarily after a quick
 * thaw is the conservative direction (work loss vs incorrect commit).
 *
 * Per spec §3.6 v0.3 F1 amend:  PG ProcessInterrupts() already returns
 * early when CritSectionCount > 0 (postgres.c:3226-3227), so this
 * hook can never fire inside a CS — no manual guard needed.
 * ============================================================ */
extern volatile sig_atomic_t ClusterFenceFreezePending;


/* ============================================================
 * GUC externs (D7 partial — defined / registered in cluster_guc.c).
 *
 *   cluster.self_fence_enabled        bool default on
 *   cluster.self_fence_grace_ms       int  1000-300000 default 30000
 *   cluster.freeze_writes_enabled     bool default on
 *   cluster.fence_audit_log           enum off/log/debug default log
 *
 * All four GUCs are PGC_POSTMASTER per Q8 user approve.  Dev escape
 * (per §3.6.1):  cluster.allow_single_node=on disables qvotec poll
 * → quorum_state stays INITIALIZING → fence path is inert regardless
 * of these four GUC settings.
 * ============================================================ */

/* Audit log enum values (cluster.fence_audit_log). */
typedef enum ClusterFenceAuditLog {
	CLUSTER_FENCE_AUDIT_LOG_OFF = 0,
	CLUSTER_FENCE_AUDIT_LOG_LOG = 1,
	CLUSTER_FENCE_AUDIT_LOG_DEBUG = 2
} ClusterFenceAuditLog;

extern bool cluster_self_fence_enabled;
extern int cluster_self_fence_grace_ms;
extern bool cluster_freeze_writes_enabled;
extern int cluster_fence_audit_log;


/* ============================================================
 * Public API.
 * ============================================================ */

/*
 * cluster_fence_shmem_size — register the fence shmem region size with
 * cluster_shmem.c (called from cluster_shmem_register_region during
 * postmaster startup before children fork).
 */
extern Size cluster_fence_shmem_size(void);

/*
 * cluster_fence_shmem_init — postmaster-once initialiser.  Allocates
 * ClusterFenceShmem private region (see cluster_fence.c for layout).
 * Idempotent via ShmemInitStruct found-flag check.  Per CLAUDE.md
 * rule 16 §postmaster-once + L19, callers must ensure
 * !IsUnderPostmaster guard at the registration site;the function
 * itself is pure shmem hookup and trusts the caller.
 */
extern void cluster_fence_shmem_init(void);

/*
 * cluster_fence_shmem_register — wires {size, init} into the
 * cluster_shmem.c registry so RequestAddinShmemSpace + ShmemInitHook
 * pick up the region during postmaster startup.  Called from
 * cluster_shmem_register_all().
 */
extern void cluster_fence_shmem_register(void);

/*
 * cluster_fence_broadcast_freeze — LMON-only callable.  Loops live
 * ProcArray entries (Step 3 D5; Step 1 stub no-op for
 * iteration) and calls SendProcSignal(pid, PROCSIG_CLUSTER_FREEZE_
 * WRITES, MyBackendId).  Updates ClusterFenceShmem.last_freeze_
 * at_us + inc cluster.fence.freeze_broadcast_count (Step 4 D11).
 * Logs per cluster.fence_audit_log GUC.  No-op if
 * cluster_freeze_writes_enabled = off.  Per Invariant I1, must be
 * invoked immediately on quorum_state OK→LOST transition (no
 * grace_ms delay).
 */
extern void cluster_fence_broadcast_freeze(const char *reason, uint64 scn);

/*
 * cluster_fence_broadcast_thaw — LMON-only callable.  Companion to
 * broadcast_freeze.  Per Invariant I2, handler-side is informational
 * only:  refreshes last_thaw_at_us + inc thaw_broadcast_count + DEBUG2
 * log;does NOT clear ClusterFenceFreezePending and does NOT bypass
 * commit gate.
 */
extern void cluster_fence_broadcast_thaw(const char *reason, uint64 scn);

/*
 * cluster_fence_self_request — LMON-only callable.  Sets
 * ClusterFenceShmem.self_fence_requested_at_us = GetCurrentTimestamp()
 * if not already set (idempotent).  Cleared by broadcast_thaw (cancel
 * pending self-fence on quorum recovery) or by postmaster after self-
 * fence initiates (one-shot).  Per Invariant I1, this is decoupled
 * from broadcast_freeze:  request is recorded the same moment freeze
 * is broadcast, but the postmaster_check delay is the only timing
 * gate for self-shutdown.
 */
extern void cluster_fence_self_request(const char *reason, uint64 scn);

/*
 * cluster_fence_lmon_tick — LMON-only call site (spec-2.28 Step 3 D5).
 *	Encapsulates quorum_state transition state machine + freeze/thaw
 *	broadcast + self_request:  LMON reads cluster_qvotec_get_quorum_
 *	state() each tick and calls this; the function tracks previous
 *	state in process-local storage and triggers the right action on
 *	OK→LOST (broadcast_freeze + self_request) or LOST→OK (broadcast_
 *	thaw, which clears self_request via §3.7 C2).
 *
 *	Per Invariant I1:  freeze broadcast IMMEDIATE on OK→LOST (no
 *	grace_ms delay).  Per Q3 = A LMON-mediated:  this is the only
 *	production caller of broadcast_freeze / _thaw / _self_request.
 */
extern void cluster_fence_lmon_tick(void);

/*
 * cluster_fence_check_interrupts — postgres.c ProcessInterrupts hook.
 * Reads-and-clears ClusterFenceFreezePending atomically; if was 1
 * AND IsTransactionState() AND cluster_freeze_writes_enabled →
 * ereport(ERROR, ERRCODE_CLUSTER_QUORUM_LOST_BACKEND, errmsg/errhint).
 *
 * Per v0.3 F1 amend:  no manual CS guard inside this body — PG
 * ProcessInterrupts() already returns early when
 * CritSectionCount > 0 (postgres.c:3226-3227), so this function
 * is unreachable inside a CS.
 *
 * Per L19 / L20:  cluster.enabled silent-skip on the first line.
 * Pre-RUNNING (initdb / single-user) returns without touching the
 * flag (the handler never set it because procsignal.c gates on
 * IsUnderPostmaster, but be defensive).
 */
extern void cluster_fence_check_interrupts(void);

/*
 * cluster_fence_postmaster_check — postmaster ServerLoop hook (D6
 * Step 3).  Reads ClusterFenceShmem.self_fence_requested_at_us;if
 * non-zero AND now - requested_at_us >= cluster_self_fence_grace_ms
 * × 1000 AND cluster_self_fence_enabled → kill(MyProcPid, SIGINT)
 * to drive PG's standard fast-shutdown path (per v0.3 F4 amend —
 * PG has no callable pmdie() function).
 *
 * Guarded by !IsUnderPostmaster (per CLAUDE.md rule 16
 * §postmaster-once + L91 EXEC_BACKEND).  Step 1 stub no-op.
 */
extern void cluster_fence_postmaster_check(void);

/*
 * cluster_get_fence_state — SRF callback for pg_cluster_fence_state
 * view.  Returns single-row 8-column SRF (D10 Step 4).  Step 1
 * not yet wired into pg_proc.dat — declared so the .c file can
 * be compiled standalone.
 */
extern Datum cluster_get_fence_state(PG_FUNCTION_ARGS);


#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_FENCE_H */
