/*-------------------------------------------------------------------------
 *
 * cluster_diag.h
 *	  pgrac DIAG (Diagnostic Process) cluster background process — Stage 1.13.
 *
 *	  Stage 1.13 ships the third cluster background process spawned by
 *	  postmaster (LMON in 1.11 was first; LCK in 1.12 was second).  Sprint A and Sprint B were
 *	  shipped together: lifecycle skeleton + GUC + SQLSTATE + inject
 *	  points + wait event + dump_diag view, all in tag v0.2.0-stage1.13.
 *	  Q2 A' (phase 4 driver split to PM_RUN) further refreshes shmem on
 *	  every SPAWNING incarnation so SQL views never report stale PID /
 *	  timestamps after a ServerLoop respawn.
 *
 *	  The DIAG main loop only does a local liveness tick
 *	  (last_liveness_tick_at advance + iter++) — it does NOT collect
 *	  cross-node diagnostic snapshots, trigger hang dumps, aggregate
 *	  cluster logs, or accept cross-node DIAG requests.  Those land in
 *	  Stage 2+ when interconnect is fully wired.
 *
 *	  Implemented surface (1.13 + 1.12.1 lessons preempted):
 *	    - cluster.diag_main_loop_interval GUC (PGC_SIGHUP, default 1000ms)
 *	    - SQLSTATE 53R0E DIAG_SPAWN_FAILED / 53R0F DIAG_NOT_READY
 *	    - 6 inject points cluster-diag-{pre-spawn,post-spawn,
 *	      ready-publish,main-loop-iter,shutdown-pre,shutdown-post}
 *	    - WAIT_EVENT_CLUSTER_BGPROC_DIAG_MAIN_LOOP wait event
 *	    - pg_cluster_state.diag view 7 keys (2 status + 5 lifecycle: status / status_enum_value /
 *	      pid / spawned_at / ready_at / last_liveness_tick_at /
 *	      main_loop_iters)
 *
 *	  HC1 (spec-1.11 §1.4): DIAG spawn entry point Asserts
 *	      !IsUnderPostmaster (postmaster-only).  DiagMain itself
 *	      Asserts IsUnderPostmaster (reverse defense in depth).
 *
 *	  HC2 (spec-1.11 §1.4): ClusterDiagStatus enum is the single
 *	      source of truth.  All status writes go through the DIAG
 *	      process itself (which holds the LWLock); postmaster only
 *	      writes shutdown_requested.
 *
 *	  HC3 (spec-1.11 §1.4): Sprint A boundary.  Real diagnostic
 *	      snapshot collection / hang dump / log aggregation / cross-node
 *	      DIAG request handling ALL deferred to Stage 2+ (interconnect
 *	      dependency).  Main loop is local liveness only.
 *
 *	  HC4 (spec-1.11 §1.4 + 4 实质 HC #2): phase_4_handler MUST have a
 *	      real reader of cluster_enabled GUC.  cluster_enabled=false →
 *	      phase_4_handler does NOT spawn DIAG (degrades to spec-1.10
 *	      stub behavior).  Tested by 063 L10.
 *
 *	  HC5 (spec-1.11 §1.4 + 4 实质 HC #3): normal shutdown via
 *	      proc_exit(0) hits PG reaper's WIFEXITED + WEXITSTATUS=0
 *	      path -> no crash recovery.  Abnormal exit (signal /
 *	      non-zero) hits HandleChildCrash -> restart_after_crash
 *	      decides instance-level crash/restart.  PG existing reaper
 *	      auto-distinguishes; this file documents the contract.
 *
 *	  HC6 (spec-1.11 §1.4 + 4 实质 HC #4): Sprint A DIAG does NOT
 *	      implement diagnostic snapshot collection or hang dumps.
 *	      Field is named last_liveness_tick_at (NOT
 *	      last_diag_snapshot_at / last_dump_emitted_at) to avoid
 *	      conflating local main-loop tick with future diagnostic
 *	      protocols (Stage 2+ cross-node DIAG).
 *
 *	  Q1 (spec-1.11 §1.4 Q-amend #1): 7-item AuxProcType integration —
 *	      enum / dispatch / start wrapper / EXEC_BACKEND argv /
 *	      BackendType + Am macro / ps display / reaper.  All wired in
 *	      Sprint A.
 *
 *	  Q2 (spec-1.11 §1.4 Q-amend #2): postmaster-owned narrow wrapper.
 *	      cluster_postmaster_start_diag() is implemented in
 *	      postmaster.c (so it can call file-static StartChildProcess);
 *	      cluster_diag_start() is a thin proxy declared here that
 *	      simply forwards to the postmaster-owned wrapper.  Call
 *	      chain: phase_4_handler → cluster_diag_start (thin) →
 *	      cluster_postmaster_start_diag (postmaster-owned) →
 *	      StartChildProcess (PG static).
 *
 *	  Q3 (spec-1.11 §1.4 Q-amend #3): readiness sync via bounded
 *	      polling (postmaster pre-ServerLoop has limited latch
 *	      infrastructure — no MyProc, shared latch ownership lifecycle
 *	      complex).  Postmaster polls shmem ready flag with
 *	      pg_usleep(100ms) loop bounded by cluster.phase4_timeout.
 *	      No latch in Sprint A; latch upgrade is reasonable Sprint B
 *	      consideration if needed.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_diag.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-1.13-diag-skeleton.md (frozen 2026-05-04, Sprint A
 *	  scope).
 *	  Foundation: spec-1.10.1 ClusterPhaseSharedState shmem layout +
 *	  spec-1.10.2 cluster_phase mirror fix +
 *	  CLAUDE.md rule 16 §Postmaster-once.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_DIAG_H
#define CLUSTER_DIAG_H

#include "datatype/timestamp.h"
#include "storage/lwlock.h"
#include "cluster/cluster_hang.h" /* spec-5.11 D1b: embedded ClusterHangSampleStore */


/*
 * ClusterDiagStatus -- HC2 SSOT for DIAG lifecycle state.
 *
 *	The numeric values are observable via SQL (Sprint B view);
 *	preserve the existing 0..4 mapping when amending.
 */
typedef enum ClusterDiagStatus {
	CLUSTER_DIAG_NOT_STARTED = 0,	/* postmaster has not yet spawned DIAG */
	CLUSTER_DIAG_SPAWNING = 1,		/* StartChildProcess returned a pid; DIAG main not yet active */
	CLUSTER_DIAG_READY = 2,			/* DIAG main loop active; phase 2 driver may advance */
	CLUSTER_DIAG_SHUTTING_DOWN = 3, /* shutdown_requested set; DIAG exiting */
	CLUSTER_DIAG_EXITED = 4			/* DIAG proc_exit complete; postmaster reaper to harvest */
} ClusterDiagStatus;

#define CLUSTER_DIAG_STATUS_LAST CLUSTER_DIAG_EXITED


/*
 * ClusterDiagSharedState -- DIAG state visible across postmaster /
 * DIAG / SQL backends.
 *
 *	Single writer for status / timestamps / iters: DIAG process itself
 *	(takes lwlock LW_EXCLUSIVE).  Postmaster writes shutdown_requested
 *	(also LW_EXCLUSIVE).  Any backend reads (LW_SHARED) for SQL view.
 *
 *	Sprint A has no Latch field.  Sprint B may add one if the bounded-
 *	polling readiness path proves limiting.
 */
typedef struct ClusterDiagSharedState {
	LWLock lwlock;					   /* LWTRANCHE_CLUSTER_DIAG guards everything below */
	ClusterDiagStatus status;		   /* HC2 SSOT */
	pid_t pid;						   /* set by DIAG in CLUSTER_DIAG_SPAWNING */
	TimestampTz spawned_at;			   /* set by DIAG in CLUSTER_DIAG_SPAWNING */
	TimestampTz ready_at;			   /* set by DIAG in CLUSTER_DIAG_READY */
	TimestampTz last_liveness_tick_at; /* HC6: local liveness tick — NOT inter-node heartbeat */
	int64 main_loop_iters;			   /* monotone counter; observable proof of liveness */
	bool shutdown_requested;		   /* postmaster sets; DIAG main loop polls + exits */

	/*
	 * spec-5.11 D1 — Hang Manager aggregate state (fulfils the
	 * last_*_at fields HC6 deliberately left out until the hang-dump duty
	 * landed).  Single writer = DIAG; guarded by the lwlock above.
	 */
	TimestampTz last_sample_at;		  /* last completed long-wait sampling round */
	TimestampTz last_dump_emitted_at; /* last hang dump written to view / log */
	int64 long_wait_count;			  /* long-waits seen in the latest round */
	int64 longest_wait_us;			  /* longest wait seen in the latest round */

	/* spec-5.11 D8 — cumulative Hang Manager counters (guarded by lwlock). */
	ClusterHangCounters hang_counters;

	/*
	 * spec-5.11 D1b — bounded shared per-row sample store (Q13-A).  Embedded
	 * directly so cluster_diag_shmem_size()/_init() grow/zero it for free and
	 * the region count is unchanged.  Guarded by the lwlock above; written by
	 * DIAG, read by backends running dump_hang.
	 */
	ClusterHangSampleStore hang_store;
} ClusterDiagSharedState;


/*
 * Public API.
 */

/*
 * Postmaster spawn helper (Q2 thin proxy).
 *
 *	Forwards to cluster_postmaster_start_diag() which lives in
 *	postmaster.c (so it can call the file-static StartChildProcess).
 *	Returns the DIAG child pid on success, or 0 on spawn failure.
 *	Asserts !IsUnderPostmaster (HC1 defense in depth).
 */
extern int cluster_diag_start(void);

/*
 * Postmaster sync wait for DIAG readiness (Q3 bounded polling).
 *
 *	Polls shmem state->status with pg_usleep(100ms) intervals up to
 *	timeout_ms.  Returns true if DIAG reaches CLUSTER_DIAG_READY in
 *	time, false on timeout.  Asserts !IsUnderPostmaster.
 */
extern bool cluster_diag_wait_for_ready(int timeout_ms);

/*
 * Postmaster shutdown signal (Q3 reverse path).
 *
 *	Sets state->shutdown_requested = true under LW_EXCLUSIVE; DIAG
 *	main loop polls this flag every iteration and proc_exit(0)s
 *	cleanly when set.  Idempotent.  Asserts !IsUnderPostmaster.
 */
extern void cluster_diag_request_shutdown(void);

/*
 * Read-only accessors for SQL view + diagnostics.  LW_SHARED.
 *
 *	Spec-1.11.1 F11: Sprint B D12 only emitted lck_status +
 *	lck_status_enum_value, leaving cluster.diag_main_loop_interval
 *	GUC unverifiable from SQL (no main_loop_iters surface).  F11
 *	completes the 6-key view with the missing 5 accessors below.
 */
extern ClusterDiagStatus cluster_diag_status(void);

/*
 * spec-5.11 D1/D1b — DIAG shared-state handle for the Hang Manager module
 * and dump_hang.  Returns NULL until the region is attached.
 */
extern ClusterDiagSharedState *cluster_diag_get_shared_state(void);

extern pid_t cluster_diag_pid(void);
extern TimestampTz cluster_diag_spawned_at(void);
extern TimestampTz cluster_diag_ready_at(void);
extern TimestampTz cluster_diag_last_liveness_tick_at(void);
extern int64 cluster_diag_main_loop_iters(void);

/*
 * Status enum -> canonical lowercase string ("not_started", "spawning",
 * "ready", "shutting_down", "exited").  Out-of-range returns
 * "(unknown)".
 */
extern const char *cluster_diag_status_to_string(ClusterDiagStatus s);

/*
 * shmem region helpers — registered by cluster_init_shmem_module()
 * via the spec-1.3 region registry.
 */
extern Size cluster_diag_shmem_size(void);
extern void cluster_diag_shmem_init(void);
extern void cluster_diag_shmem_register(void);

/*
 * AuxiliaryProcessMain dispatch entry.  HC1 reverse defense: Asserts
 * IsUnderPostmaster.  Never returns (proc_exit on shutdown).
 */
extern void DiagMain(void) pg_attribute_noreturn();


#endif /* CLUSTER_DIAG_H */
