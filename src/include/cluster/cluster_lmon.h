/*-------------------------------------------------------------------------
 *
 * cluster_lmon.h
 *	  pgrac LMON (Lock Monitor) cluster background process — Stage 1.11
 *	  Sprint A skeleton.
 *
 *	  Stage 1.11 introduces the first real cluster background process
 *	  spawned by postmaster.  Sprint A scope is the lifecycle skeleton:
 *	  AuxProcType integration / shmem state struct / readiness sync
 *	  protocol / shutdown protocol / crash semantics.  The LMON main
 *	  loop only does a local liveness tick (last_liveness_tick_at
 *	  advance + iter++) — it does NOT consume heartbeat messages, do
 *	  reconfig coordination, fence decisions, GRD maintenance, or
 *	  Recovery Coordinator triggering.  Those land in Stage 2-6.
 *
 *	  Sprint A boundary (NOT in this file):
 *	    - cluster.lmon_main_loop_interval GUC          (Sprint B)
 *	    - 53R0A LMON_SPAWN_FAILED / 53R0B LMON_NOT_READY (Sprint B)
 *	    - 6 inject points cluster-lmon-*               (Sprint B)
 *	    - WAIT_EVENT_CLUSTER_BGPROC_LMON_MAIN_LOOP wait event (Sprint B)
 *	    - pg_cluster_state.lmon view 6 keys            (Sprint B)
 *
 *	  HC1 (spec-1.11 §1.4): LMON spawn entry point Asserts
 *	      !IsUnderPostmaster (postmaster-only).  LmonMain itself
 *	      Asserts IsUnderPostmaster (reverse defense in depth).
 *
 *	  HC2 (spec-1.11 §1.4): ClusterLmonStatus enum is the single
 *	      source of truth.  All status writes go through the LMON
 *	      process itself (which holds the LWLock); postmaster only
 *	      writes shutdown_requested.
 *
 *	  HC3 (spec-1.11 §1.4): Sprint A boundary.  Real heartbeat
 *	      consumption / reconfig / fence / GRD / recovery triggering
 *	      ALL deferred to Stage 2-6.  Main loop is local liveness only.
 *
 *	  HC4 (spec-1.11 §1.4 + 4 实质 HC #2): phase_1_handler MUST have a
 *	      real reader of cluster_enabled GUC.  cluster_enabled=false →
 *	      phase_1_handler does NOT spawn LMON (degrades to spec-1.10
 *	      stub behavior).  Tested by 061 L3.
 *
 *	  HC5 (spec-1.11 §1.4 + 4 实质 HC #3): normal shutdown via
 *	      proc_exit(0) hits PG reaper's WIFEXITED + WEXITSTATUS=0
 *	      path -> no crash recovery.  Abnormal exit (signal /
 *	      non-zero) hits HandleChildCrash -> restart_after_crash
 *	      decides instance-level crash/restart.  PG existing reaper
 *	      auto-distinguishes; this file documents the contract.
 *
 *	  HC6 (spec-1.11 §1.4 + 4 实质 HC #4): Sprint A LMON does NOT
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
 *	      cluster_postmaster_start_lmon() is implemented in
 *	      postmaster.c (so it can call file-static StartChildProcess);
 *	      cluster_lmon_start() is a thin proxy declared here that
 *	      simply forwards to the postmaster-owned wrapper.  Call
 *	      chain: phase_1_handler → cluster_lmon_start (thin) →
 *	      cluster_postmaster_start_lmon (postmaster-owned) →
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
 *	  src/include/cluster/cluster_lmon.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-1.11-lmon-skeleton.md (frozen 2026-05-04, Sprint A
 *	  scope).
 *	  Foundation: spec-1.10.1 ClusterPhaseSharedState shmem layout +
 *	  spec-1.10.2 cluster_phase mirror fix +
 *	  CLAUDE.md rule 16 §Postmaster-once.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LMON_H
#define CLUSTER_LMON_H

#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/lwlock.h"


/*
 * ClusterLmonStatus -- HC2 SSOT for LMON lifecycle state.
 *
 *	The numeric values are observable via SQL (Sprint B view);
 *	preserve the existing 0..4 mapping when amending.
 */
typedef enum ClusterLmonStatus {
	CLUSTER_LMON_NOT_STARTED = 0,	/* postmaster has not yet spawned LMON */
	CLUSTER_LMON_SPAWNING = 1,		/* StartChildProcess returned a pid; LMON main not yet active */
	CLUSTER_LMON_READY = 2,			/* LMON main loop active; phase 1 driver may advance */
	CLUSTER_LMON_SHUTTING_DOWN = 3, /* shutdown_requested set; LMON exiting */
	CLUSTER_LMON_EXITED = 4			/* LMON proc_exit complete; postmaster reaper to harvest */
} ClusterLmonStatus;

#define CLUSTER_LMON_STATUS_LAST CLUSTER_LMON_EXITED


/*
 * ClusterLmonSharedState -- LMON state visible across postmaster /
 * LMON / SQL backends.
 *
 *	Single writer for status / timestamps / iters: LMON process itself
 *	(takes lwlock LW_EXCLUSIVE).  Postmaster writes shutdown_requested
 *	(also LW_EXCLUSIVE).  Any backend reads (LW_SHARED) for SQL view.
 *
 *	Sprint A has no Latch field.  Sprint B may add one if the bounded-
 *	polling readiness path proves limiting.
 */
typedef struct ClusterLmonSharedState {
	LWLock lwlock;					   /* LWTRANCHE_CLUSTER_LMON guards everything below */
	ClusterLmonStatus status;		   /* HC2 SSOT */
	pid_t pid;						   /* set by LMON in CLUSTER_LMON_SPAWNING */
	TimestampTz spawned_at;			   /* set by LMON in CLUSTER_LMON_SPAWNING */
	TimestampTz ready_at;			   /* set by LMON in CLUSTER_LMON_READY */
	TimestampTz last_liveness_tick_at; /* HC6: local liveness tick — NOT inter-node heartbeat */
	int64 main_loop_iters;			   /* monotone counter; observable proof of liveness */
	bool shutdown_requested;		   /* postmaster sets; LMON main loop polls + exits */

	/*
	 * PGRAC: spec-7.2 D1 duty-chain on-demand gating.  Dirty bitmask for
	 * the lazy-able (queue-consumption) duty families: producers set their
	 * family bit at enqueue time (fetch-or, lock-free, NOT under lwlock);
	 * the LMON main loop test-and-clears it to decide whether the drain
	 * runs this iteration.  A missed bit is latency-bounded, never a
	 * correctness issue: the heartbeat-interval floor forces every duty
	 * at >= 1 Hz (cluster.ic_duty_lazy = off restores run-every-iteration).
	 */
	pg_atomic_uint32 lazy_duty_dirty;
} ClusterLmonSharedState;

/*
 * ClusterLmonDuty -- spec-7.2 D1 classification table (§3.7).
 *
 *	EVERY tick/drain call in the LMON main-loop duty chain is enumerated
 *	here and classified by cluster_lmon_duty_is_lazy():
 *
 *	  never-lazy (correctness families):  fence / sweeps / reconfig /
 *	    recovery / epoch-quorum / order-writ members (I47: dead-sweep
 *	    MUST run BEFORE reconfig; clean-leave MUST run BEFORE reconfig).
 *	    These run every iteration, order and cadence verbatim.
 *
 *	  lazy-able (queue-consumption families):  drains whose producers
 *	    mark_dirty at enqueue time (or that are purely TTL/time-based and
 *	    ride the >= 1 Hz floor).  Skipping an iteration only delays the
 *	    drain, bounded by the floor.
 *
 *	A NEW duty added to the chain without an enum entry is never-lazy by
 *	construction (a plain unconditional call).  To make it lazy it must
 *	be enumerated + classified here, and the unit truth table
 *	(test_cluster_lmon.c) re-pinned -- that is the anti-drift gate.
 */
typedef enum ClusterLmonDuty {
	/* never-lazy (correctness families) */
	CLUSTER_LMON_DUTY_LIVENESS = 0,
	CLUSTER_LMON_DUTY_FENCE,
	CLUSTER_LMON_DUTY_GRD_DEAD_SWEEP, /* I47: MUST run BEFORE reconfig */
	CLUSTER_LMON_DUTY_GRD_RECLAIM,
	CLUSTER_LMON_DUTY_GRD_STARVATION_SWEEP,
	CLUSTER_LMON_DUTY_DEADLOCK,
	CLUSTER_LMON_DUTY_GES_REPLY_WAIT_SWEEP, /* 5.16 rule-8.A backstop */
	CLUSTER_LMON_DUTY_DIRECT_LAND_ABORTS,
	CLUSTER_LMON_DUTY_LMS_NATIVE_PROBE_RETRY,
	CLUSTER_LMON_DUTY_CLEAN_LEAVE, /* CL-I13: MUST run BEFORE reconfig */
	CLUSTER_LMON_DUTY_NODE_REMOVE, /* INV-LF1: before reconfig */
	CLUSTER_LMON_DUTY_RECONFIG,
	CLUSTER_LMON_DUTY_GRD_RECOVERY, /* AFTER reconfig tick (spec-4.6) */
	CLUSTER_LMON_DUTY_BOC_BROADCAST,
	CLUSTER_LMON_DUTY_SINVAL_RESET_ALL, /* order-sensitive protocol */
	CLUSTER_LMON_DUTY_PI_DISCARD,
	/* lazy-able (queue-consumption families) -- keep contiguous; the
	 * dirty bit is (duty - CLUSTER_LMON_DUTY_LAZY_FIRST) */
	CLUSTER_LMON_DUTY_GES_WORK_QUEUE,
	CLUSTER_LMON_DUTY_SHIP_READY,
	CLUSTER_LMON_DUTY_GRD_OUTBOUND,
	CLUSTER_LMON_DUTY_SINVAL_OUT,
	CLUSTER_LMON_DUTY_SINVAL_ACK_OUT,
	CLUSTER_LMON_DUTY_TT_HINT,
	CLUSTER_LMON_DUTY_DEDUP_TTL, /* TTL/time-based: floor-driven, no producer */
	CLUSTER_LMON_DUTY_BACKUP,
	CLUSTER_LMON_DUTY_N
} ClusterLmonDuty;

#define CLUSTER_LMON_DUTY_LAZY_FIRST CLUSTER_LMON_DUTY_GES_WORK_QUEUE

StaticAssertDecl(CLUSTER_LMON_DUTY_N - CLUSTER_LMON_DUTY_LAZY_FIRST <= 32,
				 "lazy duty families must fit the uint32 dirty bitmask");

/*
 * Duty-gating API (spec-7.2 D1).
 *
 *	cluster_lmon_duty_is_lazy      pure classification (unit-pinned).
 *	cluster_lmon_duty_mark_dirty   producer side, any process; lock-free
 *	                               fetch-or; no-op for never-lazy duties
 *	                               and pre-shmem calls.  Call BEFORE the
 *	                               cluster_lmon_wakeup() kick.
 *	cluster_lmon_duty_should_run   LMON loop side; true for never-lazy,
 *	                               lazy=off, or force_all (>= 1 Hz floor);
 *	                               otherwise test-and-clears the bit.
 */
extern bool cluster_lmon_duty_is_lazy(ClusterLmonDuty duty);
extern void cluster_lmon_duty_mark_dirty(ClusterLmonDuty duty);
extern bool cluster_lmon_duty_should_run(ClusterLmonDuty duty, bool force_all);


/*
 * Public API.
 */

/*
 * Postmaster spawn helper (Q2 thin proxy).
 *
 *	Forwards to cluster_postmaster_start_lmon() which lives in
 *	postmaster.c (so it can call the file-static StartChildProcess).
 *	Returns the LMON child pid on success, or 0 on spawn failure.
 *	Asserts !IsUnderPostmaster (HC1 defense in depth).
 */
extern int cluster_lmon_start(void);

/*
 * Postmaster sync wait for LMON readiness (Q3 bounded polling).
 *
 *	Polls shmem state->status with pg_usleep(100ms) intervals up to
 *	timeout_ms.  Returns true if LMON reaches CLUSTER_LMON_READY in
 *	time, false on timeout.  Asserts !IsUnderPostmaster.
 */
extern bool cluster_lmon_wait_for_ready(int timeout_ms);

/*
 * Postmaster shutdown signal (Q3 reverse path).
 *
 *	Sets state->shutdown_requested = true under LW_EXCLUSIVE; LMON
 *	main loop polls this flag every iteration and proc_exit(0)s
 *	cleanly when set.  Idempotent.  Asserts !IsUnderPostmaster.
 */
extern void cluster_lmon_request_shutdown(void);

/*
 * Read-only accessors for SQL view + diagnostics.  LW_SHARED.
 *
 *	Spec-1.11.1 F11: Sprint B D12 only emitted lmon_status +
 *	lmon_status_enum_value, leaving cluster.lmon_main_loop_interval
 *	GUC unverifiable from SQL (no main_loop_iters surface).  F11
 *	completes the 6-key view with the missing 5 accessors below.
 */
extern ClusterLmonStatus cluster_lmon_status(void);
extern pid_t cluster_lmon_pid(void);
extern void cluster_lmon_wakeup(void);
extern TimestampTz cluster_lmon_spawned_at(void);
extern TimestampTz cluster_lmon_ready_at(void);
extern TimestampTz cluster_lmon_last_liveness_tick_at(void);
extern int64 cluster_lmon_main_loop_iters(void);

/*
 * Status enum -> canonical lowercase string ("not_started", "spawning",
 * "ready", "shutting_down", "exited").  Out-of-range returns
 * "(unknown)".
 */
extern const char *cluster_lmon_status_to_string(ClusterLmonStatus s);

/*
 * shmem region helpers — registered by cluster_init_shmem_module()
 * via the spec-1.3 region registry.
 */
extern Size cluster_lmon_shmem_size(void);
extern void cluster_lmon_shmem_init(void);
extern void cluster_lmon_shmem_register(void);

/*
 * AuxiliaryProcessMain dispatch entry.  HC1 reverse defense: Asserts
 * IsUnderPostmaster.  Never returns (proc_exit on shutdown).
 */
extern void LmonMain(void) pg_attribute_noreturn();


#endif /* CLUSTER_LMON_H */
