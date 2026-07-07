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
#include "storage/lock.h" /* LOCKTAG / LOCKMODE for probe slot */
#include "storage/lwlock.h"
#include "cluster/cluster_grd.h" /* ClusterGrdHolderId for probe slot */


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
 *	  - lms_decision_grant_count    : grant decisions (spec-2.20 D4)
 *	  - lms_decision_reject_count   : reject decisions (spec-2.20 D4)
 *	  - lms_decision_convert_count  : convert decisions (spec-2.20 D4)
 *	  - lms_drain_empty_count   : drain pumps that found empty queue
 *	  - lms_error_count         : ereport-class errors (LMS-owned counter)
 */
/* spec-2.25 D4 — native-lock probe collector slot (HC29 + L141 reuse).
 *
 *	Each slot tracks one in-flight (LOCKTAG, lockmode) probe + N-1
 *	expected peer replies + aggregated 3-state status.  Static array
 *	in ClusterLmsSharedState sized to CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS
 *	(= 64, matches GUC max range);  GUC
 *	cluster.lms_native_lock_probe_max_inflight (default 8) bounds the
 *	"active capacity" — slots beyond it are reserved but unused.
 */
#define CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS 64

typedef struct ClusterLmsNativeLockProbeSlot {
	pg_atomic_uint64 in_use;		   /* 0 = free, 2 = initializing, 1 = active */
	uint64 probe_id;				   /* monotonic per-shard id (HC36 epoch) */
	LOCKTAG locktag;				   /* 16B PG LOCKTAG (RELATION / OBJECT) */
	LOCKMODE lockmode;				   /* 4B PG LOCKMODE */
	int32 origin_node_id;			   /* local cluster_node_id at acquire */
	int32 requester_procno;			   /* pgprocno of backend awaiting grant */
	uint32 shard_master_generation_lo; /* spec-2.27 dedup carry for async grants */
	ClusterGrdHolderId requester;	   /* HC32a exclude_holder identity */
	ClusterResId resid;				   /* grant-on-clear target resource */
	TimestampTz start_ts;			   /* dispatch timestamp */
	int32 grant_source_node_id;		   /* reply destination for async grant */
	uint32 request_opcode;			   /* original GesRequestOpcode */
	uint32 retry_count;				   /* HC32 retry-poll counter */
	uint32 expected_replies_bitmap;	   /* bit set per live peer (1 = need reply) */
	uint32 received_replies_bitmap;	   /* bit set per received reply */
	uint32 aggregated_status_packed;   /* 2 bits per node × 16 max nodes */
	uint32 final_status;			   /* sync waiter result; 0 clear,3 timeout */
	bool grant_on_clear;			   /* async remote-master grant completion */
	bool final_ready;				   /* sync waiter completion flag */
	/* spec-5.3 — for an async CONVERT (grant_on_clear + request_opcode ==
	 * GES_REQ_OPCODE_CONVERT) this carries the REDECLARE locator's current_mode
	 * so the resolve path locates the OLD holder by the precise (node, procno,
	 * current_mode) — a backend may hold multiple cluster modes on one resid
	 * (e.g. SHARE + SHARE UPDATE EXCLUSIVE), so (node, procno) alone is
	 * ambiguous.  0 (NoLock) for non-convert opcodes.  Reuses padding — slot
	 * size is unchanged. */
	uint8 convert_old_mode;
	bool _pad2[1];
	/* spec-2.27 D5 / HC55 — per-slot LWLock serializes mutation of
	 * expected/received bitmaps + aggregated_status_packed across the
	 * six concurrent paths (wait_clear backend, recv_reply handler,
	 * aggregate_and_resolve, retry_tick, cleanup_on_node_dead,
	 * cleanup_on_backend_exit).  LWLockPadded is PG's cache-aligned
	 * wrapper sized to PG_CACHE_LINE_SIZE (128B on the current build).
	 * Slot = 128B prefix + 128B LWLockPadded = 256B exactly. */
	LWLockPadded lock;
} ClusterLmsNativeLockProbeSlot;

StaticAssertDecl(sizeof(ClusterLmsNativeLockProbeSlot) == 256,
				 "ClusterLmsNativeLockProbeSlot ABI 256B lock (spec-2.27 D5 bump from 128B; "
				 "LWLockPadded = PG_CACHE_LINE_SIZE = 128B on current build)");

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
	/*
	 * spec-2.20 D4 (v0.3 frozen) — grant/reject/convert 3 NEW atomic counter
	 * (spec-2.18 §1.4 F2 deferred 真激活 wire).  Each LMS grant decision body
	 * in LWLock window inc exactly one of the 3 (mutually exclusive).
	 */
	pg_atomic_uint64 lms_decision_grant_count;
	pg_atomic_uint64 lms_decision_reject_count;
	pg_atomic_uint64 lms_decision_convert_count;
	pg_atomic_uint64 lms_drain_empty_count;
	pg_atomic_uint64 lms_error_count;

	/* spec-2.25 D4 / D13 — 7 NEW native-lock probe counters (HC29..HC36). */
	pg_atomic_uint64 native_probe_sent_count;
	pg_atomic_uint64 native_probe_reply_recv_count;
	pg_atomic_uint64 native_probe_collector_slot_full_count;
	pg_atomic_uint64 native_probe_aggregate_holder_conflict_count;
	pg_atomic_uint64 native_probe_aggregate_waiter_conflict_count;
	pg_atomic_uint64 native_probe_retry_count;
	pg_atomic_uint64 native_probe_timeout_count;

	/* probe collector slot array + monotonic id allocator (HC36). */
	pg_atomic_uint64 native_probe_next_id;
	ClusterLmsNativeLockProbeSlot native_probe_slots[CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_SLOTS];

	/* spec-2.27 HC49 / HC50 — shard master generation composite 64-bit.
	 *
	 *	lms_restart_generation:  LMS process spawn 时 bump(LmsMain entry
	 *	  + IsUnderPostmaster && MyBackendType == B_LMS guard).  Provides
	 *	  the LOW 32-bit half of shard_master_generation.
	 *
	 *	shard_master_generation = (cluster_epoch << 32) | lms_restart_generation
	 *	derived dynamically by accessor;  not cached so any post-bump
	 *	caller sees the fresh composite.
	 */
	pg_atomic_uint64 lms_restart_generation;

	/* spec-2.27 D7 / HC54 — priority starvation observability counter.
	 *
	 *	Bumped locally on retry budget 1/2 (DEBUG1) and 3/4 (WARNING).
	 *	**Never sent on wire** — reserved opcode GES_REQ_OPCODE_PRIORITY_
	 *	BOOST = 11 awaits spec-2.28+ with PG core lock manager
	 *	`LockWaitQueueInsertAtHead`改造 + integrated receiver. */
	pg_atomic_uint64 priority_starvation_observed_count;
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
 * PGRAC: spec-7.2 D2 — LMS-owned DATA-plane interconnect loop
 * (cluster_lms_data_plane.c).  startup binds the data_addr listener and
 * aims this process's tier1 state at the DATA plane (false = plane off:
 * no data_addr / cluster off / stub tier);  tick runs one management +
 * WaitEventSet round (sockets + MyLatch — replaces the historic plain
 * WaitLatch when the plane is live);  shutdown closes owned fds.
 */
extern bool cluster_lms_data_plane_startup(void);
extern bool cluster_lms_data_plane_enabled(void);
extern void cluster_lms_data_plane_tick(long timeout_ms);
extern void cluster_lms_data_plane_shutdown(void);

/*
 * PGRAC: spec-7.2 D4 — LMS wakeup (mirror of cluster_lmon_wakeup) + the
 * DATA-plane outbound ring (cluster_lms_outbound.c;  Q6-B twin ring:
 * backends stage DATA frames, LMS drains and sends on its own fds).
 */
extern void cluster_lms_wakeup(void);
extern void cluster_lms_outbound_shmem_register(void);
extern void cluster_lms_outbound_request_lwlocks(void);
extern bool cluster_lms_outbound_enqueue(uint8 msg_type, uint32 dest_node_id, const void *payload,
										 uint16 payload_len);
extern int cluster_lms_outbound_drain_send(void);
extern uint32 cluster_lms_outbound_depth(void);

/*
 * Read-only accessors for SQL view + diagnostics.
 */
extern ClusterLmsState cluster_lms_get_state(void);
extern pid_t cluster_lms_get_pid(void);
extern TimestampTz cluster_lms_get_spawned_at(void);
extern TimestampTz cluster_lms_get_ready_at(void);
extern uint64 cluster_lms_get_started_count(void);
extern uint64 cluster_lms_get_work_drained_count(void);
/*
 * spec-2.20 D9 — 3 NEW decision counter accessors (grant/reject/convert).
 * Each grant decision body inc exactly one (mutually exclusive).
 */
extern uint64 cluster_lms_get_decision_grant_count(void);
extern uint64 cluster_lms_get_decision_reject_count(void);
extern uint64 cluster_lms_get_decision_convert_count(void);
extern uint64 cluster_lms_get_drain_empty_count(void);
extern uint64 cluster_lms_get_error_count(void);

/* spec-2.25 D13 — 7 NEW native-lock probe counter accessors. */
extern uint64 cluster_lms_get_native_probe_sent_count(void);
extern uint64 cluster_lms_get_native_probe_reply_recv_count(void);
extern uint64 cluster_lms_get_native_probe_collector_slot_full_count(void);
extern uint64 cluster_lms_get_native_probe_aggregate_holder_conflict_count(void);
extern uint64 cluster_lms_get_native_probe_aggregate_waiter_conflict_count(void);
extern uint64 cluster_lms_get_native_probe_retry_count(void);
extern uint64 cluster_lms_get_native_probe_timeout_count(void);

/* spec-2.27 D1 / HC49 / HC50 — shard master generation composite + LMS
 * restart generation accessors + bump hook(only LMS aux process main
 * entry may bump;  guard inside the function). */
extern uint64 cluster_lms_get_lms_restart_generation(void);
extern uint64 cluster_lms_get_shard_master_generation(void);
extern void cluster_lms_bump_restart_generation_at_main_entry(void);

/* spec-2.27 D7 — priority starvation observability counter. */
extern void cluster_lms_inc_priority_starvation_observed(void);
extern uint64 cluster_lms_get_priority_starvation_observed_count(void);

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
 * HC4 EXACT predicate (spec-2.20 v0.3 frozen — L124 inherit from spec-2.19).
 *
 *	cluster_lms_is_ready() returns true iff lms_state == CLUSTER_LMS_READY.
 *	**禁止使用 `state >= LMS_READY` 数值比较** — DRAINING / STOPPED /
 *	DISABLED 全部不应 false-positive 匹配 READY(spec-2.18 既有
 *	cluster_lms_owns_grant() 有 latent bug 返回 READY OR DRAINING OR
 *	STOPPED;新代码必走 cluster_lms_is_ready())。
 *
 *	spec-2.20 7-step S1 entry 走此 helper 实现 HC1 fail-closed。
 */
extern bool cluster_lms_is_ready(void);

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
