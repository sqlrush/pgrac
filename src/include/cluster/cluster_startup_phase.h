/*-------------------------------------------------------------------------
 *
 * cluster_startup_phase.h
 *	  pgrac cluster postmaster startup phase machinery (Stage 1.10
 *	  skeleton).
 *
 *	  Stage 1.10 introduces a state machine that splits the previously
 *	  single cluster_init() entry into Phase 0 (pre-shmem base) ->
 *	  Phase 1 (cluster basics: interconnect listener / heartbeat /
 *	  LMON) -> Phase 2 (lock services: LMS / LMD / LCK) -> Phase 3
 *	  (recovery: startup process / Recovery Coordinator / Workers) ->
 *	  Phase 4 (normal startup: walwriter / bgwriter / DIAG / Cluster
 *	  Stats) -> RUNNING.
 *
 *	  See docs/background-process-design.md §4 (canonical startup
 *	  order), specs/spec-1.10-postmaster-startup-phase-skeleton.md,
 *	  docs/postmaster-startup-phase-design.md.
 *
 *	  Five hard constraints (spec-1.10 §1.5):
 *	    HC1 PostmasterMain-only.  cluster_run_startup_sequence() must
 *	        be called from PostmasterMain function body, NOT from
 *	        inside CreateSharedMemoryAndSemaphores() (the latter also
 *	        runs in EXEC_BACKEND children).  Every entry function in
 *	        this file Assert(!IsUnderPostmaster).
 *	    HC2 enum SSOT.  ClusterStartupPhase is the single source of
 *	        truth.  The legacy cluster_phase const char * mirror is
 *	        read-only and updated only by cluster_advance_phase().
 *	        External code MUST NOT write cluster_phase = "...".
 *	    HC3 driver/handler split.  Phase handlers do work and return
 *	        PhaseRunResult.  The central driver loop in
 *	        cluster_run_startup_sequence() decides whether to advance
 *	        based on the result.  Phase handlers MUST NOT call
 *	        cluster_advance_phase() themselves.
 *	    HC4 timeout = FATAL.  Phase transition timeouts ereport(FATAL,
 *	        errcode PGRAC_E_PHASE_TRANSITION_TIMEOUT) so postmaster
 *	        startup fails cleanly.  GUC cluster.phase{1..4}_timeout is
 *	        actually read and enforced by the driver.  Stage 1.10 stub
 *	        does not naturally trigger timeouts; cluster-startup-phase-
 *	        N-enter inject point + sleep fault simulates a stuck phase
 *	        for regression coverage.
 *	    HC5 skeleton boundary.  1.10 spawns no new background processes
 *	        (1.11-1.14 each own their respective process spawn) and
 *	        does NOT implement restart strategy (postmaster FATAL is
 *	        handed off to external supervisor: systemd / launchd /
 *	        pg_ctl).  CLUSTER_PHASE_RECONFIG is reserved for Stage 6
 *	        feature-082 reconfig phase reentry; not in this enum yet.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_startup_phase.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-1.10-postmaster-startup-phase-skeleton.md (frozen
 *	  2026-05-03 v1.1 with 5 user hard-constraint refinements).
 *	  Design: docs/postmaster-startup-phase-design.md v1.0.
 *	  Background: docs/background-process-design.md §4.
 *	  Foundation: spec-1.7.2 F2 fix (cluster_shared_fs_init WARNING
 *	  lifecycle) + CLAUDE.md rule 16 §Postmaster-once (2026-05-03).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_STARTUP_PHASE_H
#define CLUSTER_STARTUP_PHASE_H

#include "datatype/timestamp.h"


/*
 * ClusterStartupPhase -- single source of truth for postmaster startup
 * phase (HC2).  Eight values at Stage 1.10; CLUSTER_PHASE_RECONFIG is
 * a Stage 6 / feature-082 reservation (not in enum yet).
 */
typedef enum ClusterStartupPhase {
	CLUSTER_PHASE_PRE_INIT = 0, /* before cluster_init runs */
	CLUSTER_PHASE_0_BASE,		/* shmem + GUC ready, before any cluster startup */
	CLUSTER_PHASE_1_CLUSTER,	/* interconnect listener / heartbeat / LMON */
	CLUSTER_PHASE_2_LOCK,		/* LMS / LMD / LCK */
	CLUSTER_PHASE_3_RECOVERY,	/* crash recovery / Recovery Coordinator */
	CLUSTER_PHASE_4_NORMAL,		/* walwriter / bgwriter / DIAG / Cluster Stats */
	CLUSTER_PHASE_RUNNING,		/* serving SQL */
	CLUSTER_PHASE_SHUTDOWN,		/* shutdown initiated; reverse of phase enter */

	/*
	 * Stage 6 / feature-082 reservation:
	 *   CLUSTER_PHASE_RECONFIG  -- reconfig phase reentry from RUNNING.
	 *   Add as 9th enum value when reconfig lands.  Spec-1.10 does NOT
	 *   include this value (HC5 skeleton boundary).
	 */

	CLUSTER_PHASE_LAST = CLUSTER_PHASE_SHUTDOWN /* 8 values total at Stage 1.10 */
} ClusterStartupPhase;


/*
 * PhaseRunResult -- handler return status (HC3 driver/handler split).
 *
 *	Phase handlers do their work and return one of these values; the
 *	driver loop decides whether to advance based on the result.  Phase
 *	handlers MUST NOT call cluster_advance_phase() themselves.
 */
typedef enum PhaseRunResult {
	PHASE_RUN_OK,	 /* phase succeeded, driver advances */
	PHASE_RUN_RETRY, /* phase needs retry within same phase, driver does not advance */
	PHASE_RUN_FATAL	 /* phase failed unrecoverably, driver ereport(FATAL) */
} PhaseRunResult;


/*
 * Phase history ring size (HC5 fixed-size; user 修订 5).
 *
 *	Bounded to avoid Stage 6 reconfig phase reentry causing unbounded
 *	string accumulation.  Eight entries cover PRE_INIT .. SHUTDOWN with
 *	one slot of headroom for a future reconfig entry.  Older entries
 *	overwrite the oldest slot when the ring fills.
 */
#define CLUSTER_PHASE_HISTORY_RING_SIZE 8


/* ----------
 * Phase enum / accessor public API
 *
 *	cluster_startup_phase_to_string(phase) -- canonical string label
 *	    for the enum value (e.g. "phase4_normal", "shutdown").  Used
 *	    by cluster_phase legacy mirror, pg_cluster_state.phase view,
 *	    and elog DEBUG1 transition logs.  Returns "(unknown)" if the
 *	    enum value is out of range.
 *
 *	cluster_current_phase() -- returns the current ClusterStartupPhase.
 *	    Read by cluster_views (pg_cluster_state.phase) and by 1.11+
 *	    backend-process code that needs to gate on startup state.
 *
 *	cluster_phase_started_at(phase) -- returns the TimestampTz at
 *	    which the given phase was entered.  Returns 0 if the phase has
 *	    not yet been entered.  Backed by per-phase array updated only
 *	    by cluster_advance_phase().
 * ----------
 */
extern const char *cluster_startup_phase_to_string(ClusterStartupPhase phase);
extern ClusterStartupPhase cluster_current_phase(void);
extern TimestampTz cluster_phase_started_at(ClusterStartupPhase phase);


/* ----------
 * Phase transition + sequence driver
 *
 *	cluster_advance_phase(target) -- driver-internal API; HC2 SSOT
 *	    discipline.  Strict +1 transition: forward jumps and backward
 *	    transitions trigger ereport(FATAL).  Records the phase entry
 *	    timestamp + emits LOG transition message + DEBUG1 detail +
 *	    drives cluster-startup-phase-{N}-enter / -exit inject points
 *	    + updates the fixed-size phase history ring.
 *
 *	    Postmaster-only (HC1).  Phase handlers MUST NOT call this
 *	    directly (HC3); only cluster_run_startup_sequence() driver
 *	    loop calls it after a handler returns PHASE_RUN_OK.
 *
 *	cluster_run_startup_sequence() -- postmaster-only entry point
 *	    (HC1: Assert(!IsUnderPostmaster)).  Called from PostmasterMain
 *	    function body AFTER CreateSharedMemoryAndSemaphores() returns
 *	    -- NOT from inside that function (which is also called by
 *	    SubPostmasterMain on EXEC_BACKEND children).
 *
 *	    Drives Phase 0 -> 1 -> 2 -> 3 -> 4 -> RUNNING with timeout
 *	    (HC4) / failure detection / wait event / history.  Stage 1.10
 *	    skeleton: phase 1-3 handlers are no-op stubs returning
 *	    PHASE_RUN_OK immediately; phase 4 handler delegates to PG's
 *	    existing walwriter / bgwriter / etc. spawn paths (no new
 *	    process spawned by 1.10).
 *
 *	cluster_run_shutdown_sequence() -- postmaster-only entry point.
 *	    Reverse of startup: walks back to CLUSTER_PHASE_SHUTDOWN.
 *	    Stage 1.10 stub.
 * ----------
 */
extern void cluster_advance_phase(ClusterStartupPhase target);
extern void cluster_run_startup_sequence(void);
extern void cluster_run_shutdown_sequence(void);


/* ----------
 * Diagnostic accessors (read-only, called by cluster_views.c /
 * cluster_debug.c to populate pg_cluster_state.phase).
 *
 *	cluster_phase_elapsed_seconds() -- seconds since the current phase
 *	    was entered.  Returns 0 in CLUSTER_PHASE_PRE_INIT.
 *
 *	cluster_phase_history_format(buf, size) -- formats the fixed-size
 *	    phase history ring into a comma-separated string
 *	    "phase0@2026-...,phase1@2026-..." up to CLUSTER_PHASE_HISTORY_
 *	    RING_SIZE entries.  Caller-provided buffer.
 * ----------
 */
extern int64 cluster_phase_elapsed_seconds(void);
extern void cluster_phase_history_format(char *buf, size_t size);


#endif /* CLUSTER_STARTUP_PHASE_H */
