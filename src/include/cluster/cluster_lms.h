/*-------------------------------------------------------------------------
 *
 * cluster_lms.h
 *	  pgrac LMS (Lock Master / Grant Service) cluster background process —
 *	  spec-2.18 Sprint A skeleton.
 *
 *	  Spec-2.18 is the first GES production-activation sub-spec under
 *	  Phase 2.C.  Sprint A scope is the lifecycle skeleton + grant-decision
 *	  ownership migration from LMON to LMS — single ownership path with
 *	  fail-closed semantics (no runtime LMON fallback grant).  The LMS
 *	  main loop drains the ges work_queue and uses the proven aux-process
 *	  WaitLatch idle pattern in the skeleton.  Producer-side
 *	  ConditionVariable wake API is retained for the later event-driven
 *	  LMS path.  Real 7-step state machine activation,
 *	  BAST send/receive, deadlock Tarjan, cleanup_on_backend_exit entry
 *	  sweep, lock class expansion (TRANSACTION/RELATION/OBJECT), master
 *	  tombstone are NOT in this spec — they all land in spec-2.19+.
 *
 *	  Sprint A boundary (NOT in this file):
 *	    - cluster.lms_max_drain_batch GUC max-batch processing (Step 4)
 *	    - 53R80 cluster_lms_unavailable SQLSTATE (Step 4)
 *	    - WAIT_EVENT_CLUSTER_LMS_STARTUP/DRAIN/IDLE wait events (Step 4)
 *	    - pg_cluster_lms view 4-state分流 (Step 4)
 *	    - dump_cluster_lms 6 emit_row + cluster_unit T-lms-1..8 (Step 5)
 *
 *	  HC1 (spec-2.18 §1.4.5): LMS unavailable fail-closed hard contract.
 *	      LMS_READY 后 LMON grant path hard-disabled; LMS crash → backend
 *	      receives SQLSTATE 53R80; runtime NO ownership transfer path
 *	      (violates L98/L108/L116 family).  cluster.lms_enabled=off is
 *	      PGC_POSTMASTER startup-only fallback (restart 才生效).
 *
 *	  HC2 (spec-2.18 §1.4.6): LMS 4-state semantic split.  pg_cluster_lms
 *	      view + 53R80 raise path 必区分:
 *	        (a) DISABLED — lms_enabled=off LMS process 不 fork
 *	        (b) NOT_STARTED / STARTING — enabled=on postmaster 起来但
 *	            LMS 未 spawn 完成;LMON drain bootstrap backlog catch-up
 *	        (c) READY / DRAINING — LMS 主导 grant;LMON grant path
 *	            hard-disabled
 *	        (d) STOPPED / unavailable — LMS 死;reason='crashed_unavailable'
 *	            53R80 raise
 *
 *	  HC3 (spec-2.18 §1.4.4): ConditionVariable producer API.
 *	    (a) producer 成功 enqueue 后 ConditionVariableBroadcast (在
 *	        LWLock release 之后, 避免 wake 后 LMS reacquire spin)
 *	    (b) LMS skeleton consumer 使用 WaitLatch 轮询空队列,不进入 CV
 *	        sleeper list;event-driven CV consumer 推 production activation
 *	    (c) 不在 async-signal-unsafe critical path 做 ConditionVariable 操作
 *
 *	  HC4 (spec-2.18 §1.4.3): single ownership atomic guard.
 *	      LMON tick body 入口
 *	          if (atomic_read(LmsShmem->lms_state) >= LMS_READY) return early;
 *	      不引入 lms_drain_owner 第二 ownership field
 *	      (避免 LMS/LMON ownership 分裂 violating F1 single ownership).
 *
 *	  HC5 (spec-2.18 §1.4.7): NUM_AUXILIARY_PROCS bump 必走.
 *	      LMS 是第 7 个 cluster aux process; src/include/storage/proc.h
 *	      USE_PGRAC_CLUSTER 块的 NUM_AUXILIARY_PROCS bump 12 → 13.
 *	      StaticAssertDecl(NUM_AUXILIARY_PROCS >= NUM_AUXPROCTYPES)
 *	      防本地编译过 cluster init / TAP baseline 才爆.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_lms.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-2.18-lms-daemon-grant-ownership-migration.md
 *	  (FROZEN v0.3 2026-05-14 user approve, Sprint A scope).
 *	  Anchor: cluster_lmon.h (spec-1.11) for skeleton pattern; LMS adds
 *	  drain consumer + DISABLED state.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LMS_H
#define CLUSTER_LMS_H

#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"


/*
 * ClusterLmsState -- HC2 4-state semantic SSOT.
 *
 *	NUMERIC values are observable via SQL view (Step 4); preserve
 *	mapping when amending.  DISABLED=5 is appended after STOPPED to
 *	preserve 0..4 mapping for non-disabled states (parallels LMON
 *	enum compatibility convention).
 *
 *	State transitions (monotonic except DISABLED which is set once at
 *	startup time and never transitions):
 *	    DISABLED                 (cluster.lms_enabled=off at startup)
 *	    NOT_STARTED -> STARTING -> READY -> DRAINING -> STOPPED
 *
 *	HC1 invariant: LMON grant path hard-disabled when state is READY /
 *	DRAINING / STOPPED.  DISABLED is startup opt-out and keeps LMON
 *	fallback.
 */
typedef enum ClusterLmsState {
	CLUSTER_LMS_NOT_STARTED = 0, /* postmaster has not yet spawned LMS */
	CLUSTER_LMS_STARTING = 1,	 /* StartChildProcess returned pid; LMS main not yet active */
	CLUSTER_LMS_READY = 2,		 /* LMS main loop active; owns grant decision */
	CLUSTER_LMS_DRAINING = 3,	 /* shutdown_requested set; draining work_queue */
	CLUSTER_LMS_STOPPED = 4,	 /* LMS proc_exit complete; postmaster reaper to harvest */
	CLUSTER_LMS_DISABLED = 5	 /* cluster.lms_enabled=off startup-only; LMS process 不 fork */
} ClusterLmsState;

#define CLUSTER_LMS_STATE_LAST CLUSTER_LMS_DISABLED


/*
 * ClusterLmsSharedState -- LMS state visible across postmaster / LMS /
 * SQL backends.
 *
 *	HC4 single ownership: lms_state atomic; LMON tick body calls
 *	cluster_lms_owns_grant() to distinguish bootstrap fallback from
 *	runtime fail-closed.
 *	不引入 lms_drain_owner 第二字段.
 *
 *	HC3 ConditionVariable producer API: producers (LMON tick / ges request
 *	handler / cleanup_on_node_dead) broadcast cv after successful enqueue.
 *	The Step 6 LMS skeleton drains by polling with WaitLatch; the CV field
 *	is retained as the stable wake substrate for later production activation.
 *
 *	Counter discipline (6 only — spec-2.18 §1.4 F2 收紧;
 *	grant/reject/convert 分项推 spec-2.20 真激活 grant state machine):
 *	  - lms_started_count       : LMS startup events (monotone)
 *	  - lms_ready_at_us         : TimestampTz of last READY transition
 *	  - lms_work_drained_count  : successful work_queue drain pumps
 *	  - lms_decision_count      : total grant decisions (skeleton stub)
 *	  - lms_drain_empty_count   : drain pumps that found empty queue
 *	  - lms_error_count         : ereport-class errors (LMS-owned counter)
 */
typedef struct ClusterLmsSharedState {
	LWLock lwlock;				/* LWTRANCHE_CLUSTER_LMS guards non-atomic fields */
	pg_atomic_uint32 lms_state; /* ClusterLmsState atomic (HC4 single ownership field) */
	pid_t pid;					/* set by LMS in STARTING */
	TimestampTz spawned_at;		/* set by LMS in STARTING */
	TimestampTz ready_at;		/* set by LMS in READY */
	TimestampTz stopped_at;		/* set by LMS in STOPPED */
	bool shutdown_requested;	/* postmaster sets; LMS main loop polls + exits */

	/* HC3 ConditionVariable wake substrate */
	ConditionVariable cv;
	pg_atomic_uint32 work_queue_count; /* placeholder; spec-2.20+ real wire */

	/* 6 counter (atomic) — HC2 / spec-2.18 §1.4 F2 */
	pg_atomic_uint64 lms_started_count;
	pg_atomic_uint64 lms_ready_at_us;
	pg_atomic_uint64 lms_work_drained_count;
	pg_atomic_uint64 lms_decision_count;
	pg_atomic_uint64 lms_drain_empty_count;
	pg_atomic_uint64 lms_error_count;
} ClusterLmsSharedState;


/*
 * Public API.
 */

/*
 * Postmaster spawn helper (Q2 thin proxy mirror of LMON).
 *
 *	Forwards to cluster_postmaster_start_lms() which lives in
 *	postmaster.c.  Returns the LMS child pid on success, or 0 on
 *	spawn failure / lms_enabled=off (DISABLED state at startup).
 *	Asserts !IsUnderPostmaster.
 */
extern int cluster_lms_start(void);

/*
 * Postmaster sync wait for LMS readiness (Q3 bounded polling).
 *
 *	Returns true when state reaches READY within timeout_ms.  If
 *	lms_enabled=off at startup, returns true immediately
 *	(DISABLED state is a legitimate "ready-or-skip" terminal).
 */
extern bool cluster_lms_wait_for_ready(int timeout_ms);

/*
 * Postmaster shutdown signal.  Idempotent.  Asserts !IsUnderPostmaster.
 */
extern void cluster_lms_request_shutdown(void);

/*
 * LMS main entry — invoked from auxprocess.c dispatch.  Asserts
 * IsUnderPostmaster (reverse defense in depth).  Never returns.
 */
extern void LmsMain(void) pg_attribute_noreturn();

/*
 * Read-only accessors for SQL view + diagnostics.
 */
extern ClusterLmsState cluster_lms_get_state(void);
extern pid_t cluster_lms_get_pid(void);
extern TimestampTz cluster_lms_get_spawned_at(void);
extern TimestampTz cluster_lms_get_ready_at(void);
extern uint64 cluster_lms_get_started_count(void);
extern uint64 cluster_lms_get_work_drained_count(void);
extern uint64 cluster_lms_get_decision_count(void);
extern uint64 cluster_lms_get_drain_empty_count(void);
extern uint64 cluster_lms_get_error_count(void);

/*
 * State enum -> canonical lowercase string ("disabled", "not_started",
 * "starting", "ready", "draining", "stopped").  Out-of-range returns
 * "(unknown)".  4-state view分流 maps to this output (HC2).
 */
extern const char *cluster_lms_state_to_string(ClusterLmsState s);

/*
 * HC4 single ownership atomic check.  LMON tick body entry calls this
 * to determine whether LMS owns grant decisions.  Returns true for READY,
 * DRAINING, and STOPPED; returns false for DISABLED so startup opt-out keeps
 * LMON fallback.  Read-only atomic load.
 */
extern bool cluster_lms_owns_grant(void);

/*
 * HC3 producer wake — broadcast cv after successful work_queue enqueue.
 * The Step 6 LMS skeleton does not yet wait on this CV; it is kept as the
 * stable producer-side API for the production LMS consumer.  No-op if LMS
 * DISABLED.
 */
extern void cluster_lms_wake_drain(void);

/*
 * shmem region helpers — registered by cluster_init_shmem_module()
 * via the spec-1.3 region registry.
 */
extern Size cluster_lms_shmem_size(void);
extern void cluster_lms_shmem_request(void);
extern void cluster_lms_shmem_init(void);
extern void cluster_lms_shmem_register(void);
extern ClusterLmsSharedState *cluster_lms_shared_state(void);

#endif /* CLUSTER_LMS_H */
