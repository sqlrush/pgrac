/*-------------------------------------------------------------------------
 *
 * cluster_lck.h
 *	  pgrac LCK (Lock Process) cluster background process — Stage 1.12
 *	  Sprint A skeleton.
 *
 *	  Stage 1.12 introduces the first real cluster background process
 *	  spawned by postmaster.  Sprint A scope is the lifecycle skeleton:
 *	  AuxProcType integration / shmem state struct / readiness sync
 *	  protocol / shutdown protocol / crash semantics.  The LCK main
 *	  loop only does a local liveness tick (last_liveness_tick_at
 *	  advance + iter++) — it does NOT consume heartbeat messages, do
 *	  reconfig coordination, fence decisions, GRD maintenance, or
 *	  Recovery Coordinator triggering.  Those land in Stage 2-6.
 *
 *	  Sprint A boundary (NOT in this file):
 *	    - cluster.lck_main_loop_interval GUC          (Sprint B)
 *	    - 53R0C LCK_SPAWN_FAILED / 53R0D LCK_NOT_READY (Sprint B)
 *	    - 6 inject points cluster-lck-*               (Sprint B)
 *	    - WAIT_EVENT_CLUSTER_BGPROC_LCK_MAIN_LOOP wait event (Sprint B)
 *	    - pg_cluster_state.lck view 6 keys            (Sprint B)
 *
 *	  HC1 (spec-1.11 §1.4): LCK spawn entry point Asserts
 *	      !IsUnderPostmaster (postmaster-only).  LckMain itself
 *	      Asserts IsUnderPostmaster (reverse defense in depth).
 *
 *	  HC2 (spec-1.11 §1.4): ClusterLckStatus enum is the single
 *	      source of truth.  All status writes go through the LCK
 *	      process itself (which holds the LWLock); postmaster only
 *	      writes shutdown_requested.
 *
 *	  HC3 (spec-1.11 §1.4): Sprint A boundary.  Real heartbeat
 *	      consumption / reconfig / fence / GRD / recovery triggering
 *	      ALL deferred to Stage 2-6.  Main loop is local liveness only.
 *
 *	  HC4 (spec-1.11 §1.4 + 4 实质 HC #2): phase_2_handler MUST have a
 *	      real reader of cluster_enabled GUC.  cluster_enabled=false →
 *	      phase_2_handler does NOT spawn LCK (degrades to spec-1.10
 *	      stub behavior).  Tested by 061 L3.
 *
 *	  HC5 (spec-1.11 §1.4 + 4 实质 HC #3): normal shutdown via
 *	      proc_exit(0) hits PG reaper's WIFEXITED + WEXITSTATUS=0
 *	      path -> no crash recovery.  Abnormal exit (signal /
 *	      non-zero) hits HandleChildCrash -> restart_after_crash
 *	      decides instance-level crash/restart.  PG existing reaper
 *	      auto-distinguishes; this file documents the contract.
 *
 *	  HC6 (spec-1.11 §1.4 + 4 实质 HC #4): Sprint A LCK does NOT
 *	      implement inter-node heartbeat.  Field is named
 *	      last_liveness_tick_at (NOT last_heartbeat_at) to avoid
 *	      conflating local main-loop tick with future cluster-wide
 *	      heartbeat protocol (spec-1.15+ Heartbeat process).
 *
 *	  Q1 (spec-1.11 §1.4 Q-amend #1): 7-item AuxProcType integration —
 *	      enum / dispatch / start wrapper / EXEC_BACKEND argv /
 *	      BackendType + Am macro / ps display / reaper.  All wired in
 *	      Sprint A.
 *
 *	  Q2 (spec-1.11 §1.4 Q-amend #2): postmaster-owned narrow wrapper.
 *	      cluster_postmaster_start_lck() is implemented in
 *	      postmaster.c (so it can call file-static StartChildProcess);
 *	      cluster_lck_start() is a thin proxy declared here that
 *	      simply forwards to the postmaster-owned wrapper.  Call
 *	      chain: phase_2_handler → cluster_lck_start (thin) →
 *	      cluster_postmaster_start_lck (postmaster-owned) →
 *	      StartChildProcess (PG static).
 *
 *	  Q3 (spec-1.11 §1.4 Q-amend #3): readiness sync via bounded
 *	      polling (postmaster pre-ServerLoop has limited latch
 *	      infrastructure — no MyProc, shared latch ownership lifecycle
 *	      complex).  Postmaster polls shmem ready flag with
 *	      pg_usleep(100ms) loop bounded by cluster.phase1_timeout.
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
 *	  src/include/cluster/cluster_lck.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-1.12-lck-skeleton.md (frozen 2026-05-04, Sprint A
 *	  scope).
 *	  Foundation: spec-1.10.1 ClusterPhaseSharedState shmem layout +
 *	  spec-1.10.2 cluster_phase mirror fix +
 *	  CLAUDE.md rule 16 §Postmaster-once.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LCK_H
#define CLUSTER_LCK_H

#include "datatype/timestamp.h"
#include "storage/lwlock.h"


/*
 * ClusterLckStatus -- HC2 SSOT for LCK lifecycle state.
 *
 *	The numeric values are observable via SQL (Sprint B view);
 *	preserve the existing 0..4 mapping when amending.
 */
typedef enum ClusterLckStatus {
	CLUSTER_LCK_NOT_STARTED = 0,	/* postmaster has not yet spawned LCK */
	CLUSTER_LCK_SPAWNING = 1,		/* StartChildProcess returned a pid; LCK main not yet active */
	CLUSTER_LCK_READY = 2,			/* LCK main loop active; phase 2 driver may advance */
	CLUSTER_LCK_SHUTTING_DOWN = 3, /* shutdown_requested set; LCK exiting */
	CLUSTER_LCK_EXITED = 4			/* LCK proc_exit complete; postmaster reaper to harvest */
} ClusterLckStatus;

#define CLUSTER_LCK_STATUS_LAST CLUSTER_LCK_EXITED


/*
 * ClusterLckSharedState -- LCK state visible across postmaster /
 * LCK / SQL backends.
 *
 *	Single writer for status / timestamps / iters: LCK process itself
 *	(takes lwlock LW_EXCLUSIVE).  Postmaster writes shutdown_requested
 *	(also LW_EXCLUSIVE).  Any backend reads (LW_SHARED) for SQL view.
 *
 *	Sprint A has no Latch field.  Sprint B may add one if the bounded-
 *	polling readiness path proves limiting.
 */
typedef struct ClusterLckSharedState {
	LWLock lwlock;					   /* LWTRANCHE_CLUSTER_LCK guards everything below */
	ClusterLckStatus status;		   /* HC2 SSOT */
	pid_t pid;						   /* set by LCK in CLUSTER_LCK_SPAWNING */
	TimestampTz spawned_at;			   /* set by LCK in CLUSTER_LCK_SPAWNING */
	TimestampTz ready_at;			   /* set by LCK in CLUSTER_LCK_READY */
	TimestampTz last_liveness_tick_at; /* HC6: local liveness tick — NOT inter-node heartbeat */
	int64 main_loop_iters;			   /* monotone counter; observable proof of liveness */
	bool shutdown_requested;		   /* postmaster sets; LCK main loop polls + exits */
} ClusterLckSharedState;


/*
 * Public API.
 */

/*
 * Postmaster spawn helper (Q2 thin proxy).
 *
 *	Forwards to cluster_postmaster_start_lck() which lives in
 *	postmaster.c (so it can call the file-static StartChildProcess).
 *	Returns the LCK child pid on success, or 0 on spawn failure.
 *	Asserts !IsUnderPostmaster (HC1 defense in depth).
 */
extern int cluster_lck_start(void);

/*
 * Postmaster sync wait for LCK readiness (Q3 bounded polling).
 *
 *	Polls shmem state->status with pg_usleep(100ms) intervals up to
 *	timeout_ms.  Returns true if LCK reaches CLUSTER_LCK_READY in
 *	time, false on timeout.  Asserts !IsUnderPostmaster.
 */
extern bool cluster_lck_wait_for_ready(int timeout_ms);

/*
 * Postmaster shutdown signal (Q3 reverse path).
 *
 *	Sets state->shutdown_requested = true under LW_EXCLUSIVE; LCK
 *	main loop polls this flag every iteration and proc_exit(0)s
 *	cleanly when set.  Idempotent.  Asserts !IsUnderPostmaster.
 */
extern void cluster_lck_request_shutdown(void);

/*
 * Read-only accessors for SQL view + diagnostics.  LW_SHARED.
 *
 *	Spec-1.11.1 F11: Sprint B D12 only emitted lck_status +
 *	lck_status_enum_value, leaving cluster.lck_main_loop_interval
 *	GUC unverifiable from SQL (no main_loop_iters surface).  F11
 *	completes the 6-key view with the missing 5 accessors below.
 */
extern ClusterLckStatus cluster_lck_status(void);
extern pid_t cluster_lck_pid(void);
extern TimestampTz cluster_lck_spawned_at(void);
extern TimestampTz cluster_lck_ready_at(void);
extern TimestampTz cluster_lck_last_liveness_tick_at(void);
extern int64 cluster_lck_main_loop_iters(void);

/*
 * Status enum -> canonical lowercase string ("not_started", "spawning",
 * "ready", "shutting_down", "exited").  Out-of-range returns
 * "(unknown)".
 */
extern const char *cluster_lck_status_to_string(ClusterLckStatus s);

/*
 * shmem region helpers — registered by cluster_init_shmem_module()
 * via the spec-1.3 region registry.
 */
extern Size cluster_lck_shmem_size(void);
extern void cluster_lck_shmem_init(void);
extern void cluster_lck_shmem_register(void);

/*
 * AuxiliaryProcessMain dispatch entry.  HC1 reverse defense: Asserts
 * IsUnderPostmaster.  Never returns (proc_exit on shutdown).
 */
extern void LckMain(void) pg_attribute_noreturn();


#endif /* CLUSTER_LCK_H */
