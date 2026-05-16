/*-------------------------------------------------------------------------
 *
 * cluster_lmd.h
 *	  pgrac LMD (Lock Manager Daemon — deadlock detection actor) cluster
 *	  background process — spec-2.19 Sprint A skeleton.
 *
 *	  Spec-2.19 is the second GES production-activation sub-spec under
 *	  Phase 2.C (Sprint A第 2 站 after spec-2.18 LMS).  Sprint A scope is
 *	  the lifecycle skeleton + deadlock-detection ownership migration
 *	  from spec-2.17 caller-side 4-node placeholder to LMD — single
 *	  ownership path with fail-closed semantics (no runtime caller-side
 *	  fallback;HC1).  The LMD producer API broadcasts its
 *	  ConditionVariable;the no-graph skeleton loop observes producer
 *	  deltas on a bounded latch tick and increments skeleton counters
 *	  only.  Real Tarjan cycle detection,
 *	  wait-for graph maintenance, victim selection, cancellation are NOT
 *	  in this spec — they all land in spec-2.20+ (7-step state machine
 *	  activation wire callsite) and spec-5.9 (victim + cancellation).
 *
 *	  Sprint A boundary (NOT in this file — see deliverables D10-D15):
 *	    - 53R81 cluster_lmd_unavailable SQLSTATE (Step 4 D12)
 *	    - WAIT_EVENT_CLUSTER_LMD_STARTUP / SCAN / IDLE wait events (D12)
 *	    - cluster.lmd_enabled GUC PGC_POSTMASTER (D12)
 *	    - pg_cluster_lmd view 4-state分流 + SRF cluster_get_lmd_state() (D11)
 *	    - dump_cluster_lmd 7 emit_row 'lmd' category (D10)
 *	    - cluster_unit T-lmd-1..8 (D13)
 *	    - cluster_tap 106_lmd_smoke.pl + L122 alphabetic verify (D14)
 *
 *	  HC1 (spec-2.19 §1.4.5 — v0.2 P1.3 硬化):LMD unavailable fail-closed
 *	      hard contract.  enabled=on 时所有非 READY state(NOT_STARTED /
 *	      STARTING / DRAINING / STOPPED)全部 53R81 raise + caller retry;
 *	      只有 enabled=off PGC_POSTMASTER startup-time fallback 才走
 *	      caller-side legacy.  Runtime SET 无效;无 runtime ownership
 *	      transfer path(violates L98/L108/L116/L120 family).
 *
 *	  HC2 (spec-2.19 §1.4.6):LMD 4-state semantic split.  pg_cluster_lmd
 *	      view + 53R81 raise path 必区分:
 *	        (a) DISABLED — lmd_enabled=off LMD process 不 fork(唯一走
 *	            caller-side legacy 路径)
 *	        (b) NOT_STARTED / STARTING / DRAINING / STOPPED — enabled=on
 *	            但 LMD 不在 READY → 全部 53R81 raise + caller retry
 *	            (禁止 silent fallback)
 *	        (c) READY — LMD 主导;caller-side legacy hard-disabled
 *	        (d) CRASHED — process 不存在但 enabled=on → 53R81
 *
 *	  HC3 (spec-2.19 §1.4.4 — L121 spec-2.18 v0.3 L2.8 inherit):
 *	      ConditionVariable producer-side wake 契约.
 *	    (a) cluster_lmd_submit_wait_edge() 递增 submission_count 后
 *	        立即 ConditionVariableBroadcast(&cv)(不持 LMD LWLock;不得
 *	        处在 signal handler / critical section)
 *	    (b) LMD skeleton main loop 必用 submission_count delta 防
 *	        spurious wakeup,但当前不注册 CV sleeper;只走 bounded latch
 *	        idle path.  Production graph-maintenance spec 若改成真实
 *	        CV consumer,必须同 spec 补 while-loop sleep +
 *	        ConditionVariableCancelSleep shutdown path.
 *	    (c) 不在 async-signal-unsafe critical path 做 ConditionVariable
 *	        操作(palloc / elog / LWLock-held)
 *
 *	  HC4 (spec-2.19 §1.4.5 + v0.3 codex P1.5):single ownership exact
 *	      predicate.  Caller-side 4-node placeholder 入口
 *	          if (cluster_lmd_is_ready())
 *	              cluster_lmd_submit_wait_edge(...);
 *	          else if (!cluster_lmd_enabled)
 *	              goto caller_side_legacy_path;
 *	          else
 *	              ereport(ERROR, ERRCODE_CLUSTER_LMD_UNAVAILABLE);
 *	      **禁止 `state >= LMD_READY` 数值比较**:enum 不连续值
 *	      (DRAINING=3 / STOPPED=4 / DISABLED=5)会让 `>=` 误判;必走 exact
 *	      `state == LMD_READY` 或 cluster_lmd_is_ready() helper.
 *
 *	  HC5 (spec-2.19 §1.4.7 — I11 inherit spec-2.18):NUM_AUXILIARY_PROCS
 *	      bump 必走.  LMD 是第 8 个 cluster aux process;
 *	      src/include/storage/proc.h USE_PGRAC_CLUSTER 块的
 *	      NUM_AUXILIARY_PROCS bump 13 → 14.  StaticAssertDecl 配套防御
 *	      "本地编译过 cluster init / TAP baseline 才爆".
 *
 *	  HC6 (spec-2.19 §1.4.3 — v0.2 P1.2 + v0.3 P1.6):skeleton 占位不等于
 *	      假装工作.  LMD 不保存 wait edge,不维护 ring/hash/queue,不
 *	      声明 dequeue consumer;cluster_lmd_submit_wait_edge() 调用即
 *	      ++lmd_edge_submission_count + ConditionVariableBroadcast(cv);
 *	      LMD main loop wake 后只观察 submission_count delta 并 ++
 *	      wake/idle/error 等 skeleton counters(real graph maintenance
 *	      + Tarjan 推 spec-2.20+ 与 producer/consumer 同 spec ship,
 *	      L114 inherit).
 *
 *	  HC7 (spec-2.19 §1.4.8 — L121 NEW spec-2.18 F1 inherit):auxprocess.c
 *	      LmdProcess branch 必早期 install SIGTERM/SIGINT/SIGHUP handlers
 *	      + sigprocmask UnBlockSig **BEFORE pgstat_bestart /
 *	      pgstat-visible publication / any LMD shmem-LWLock-CV operation**.
 *	      LMS F1 实证:startup window pre-pgstat-publish SIGTERM 丢失 →
 *	      CleanupProcSignalState pss_barrierCV broadcast hang(60s pg_ctl
 *	      stop 卡死 833/833 samples).
 *
 *	  I14 (spec-2.19 §1.4 P1.4 — L114 family):LMD skeleton MUST NOT
 *	      register new ProcSignal slot(用 ConditionVariable wake);若未来
 *	      sub-spec(spec-2.20+)需引入 ProcSignal MUST 同 spec ship 完整
 *	      register + cleanup + shutdown 语义(producer-consumer lifecycle
 *	      闭环).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_lmd.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-2.19-lmd-daemon-deadlock-ownership-migration.md
 *	  (FROZEN v0.3 2026-05-14 user approve, Sprint A scope).
 *	  Anchor: cluster_lms.h (spec-2.18) for skeleton pattern;LMD adds
 *	  producer-side ConditionVariable broadcast but keeps the aux-process
 *	  latch idle loop until spec-2.20+ wires the real graph consumer.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LMD_H
#define CLUSTER_LMD_H

#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"


/*
 * ClusterLmdState -- HC2 4-state semantic SSOT.
 *
 *	NUMERIC values are observable via SQL view (Step 4); preserve
 *	mapping when amending.  DISABLED=5 is appended after STOPPED to
 *	preserve 0..4 mapping for non-disabled states (parallels LMS
 *	enum compatibility convention).
 *
 *	State transitions (单个 LMD process generation 内 monotonic;
 *	postmaster reaper restart 形成新 generation,可从 STOPPED 回
 *	STARTING/READY,由 lmd_started_count / lmd_ready_at_us 区分):
 *	    DISABLED                 (cluster.lmd_enabled=off at startup)
 *	    NOT_STARTED -> STARTING -> READY -> DRAINING -> STOPPED
 *
 *	HC4 exact predicate:caller-side path gates on
 *	`cluster_lmd_is_ready()` which returns true iff state == READY.
 *	**禁止 `state >= LMD_READY` 数值比较** — DRAINING / STOPPED /
 *	DISABLED 全部不应 false-positive 匹配 READY.
 */
typedef enum ClusterLmdState {
	CLUSTER_LMD_NOT_STARTED = 0, /* postmaster has not yet spawned LMD */
	CLUSTER_LMD_STARTING = 1,	 /* StartChildProcess returned pid; LMD main not yet active */
	CLUSTER_LMD_READY = 2,		 /* LMD main loop active; owns deadlock detection */
	CLUSTER_LMD_DRAINING = 3,	 /* shutdown_requested set; draining wake events */
	CLUSTER_LMD_STOPPED = 4,	 /* LMD proc_exit complete; postmaster reaper to harvest */
	CLUSTER_LMD_DISABLED = 5	 /* cluster.lmd_enabled=off startup-only; LMD process 不 fork */
} ClusterLmdState;

#define CLUSTER_LMD_STATE_LAST CLUSTER_LMD_DISABLED


/*
 * ClusterLmdSharedState -- LMD state visible across postmaster / LMD /
 * SQL backends.
 *
 *	HC4 single ownership:lmd_state atomic;caller-side path calls
 *	cluster_lmd_is_ready() (exact == READY) to distinguish bootstrap /
 *	disabled fallback from runtime fail-closed.
 *
 *	HC3 ConditionVariable producer-consumer wake:producers
 *	(spec-2.17 caller-side placeholder → submit_wait_edge stub,
 *	spec-2.20+ LMS S4 wait detect / cleanup_on_backend_exit /
 *	reconfig drain) broadcast cv after successful submission_count++.
 *	LMD main loop uses the submission_count delta while-loop pattern
 *	(HC3 (b)) — wake when seen_submission_count < current value,
 *	idle otherwise.
 *
 *	HC6 skeleton 不保存 wait edge:no ring buffer / hash table / queue
 *	占位.  cluster_lmd_submit_wait_edge() 调用即 atomic ++ + CV
 *	broadcast;LMD wake observes submission_count delta only.
 *
 *	Counter discipline (6 only — spec-2.19 §0 Q8;
 *	add_edge / remove_edge / cycle_detected / victim_selected 分项推
 *	spec-2.20+ 真激活 Tarjan):
 *	  - lmd_started_count            : LMD startup events (monotone)
 *	  - lmd_ready_at_us              : TimestampTz of last READY transition
 *	  - lmd_edge_submission_count    : submit_wait_edge invocations (skeleton stub)
 *	  - lmd_wake_count               : CV wake-up events (delta observed)
 *	  - lmd_idle_count               : idle timeouts (no new submission seen)
 *	  - lmd_error_count              : ereport-class errors (LMD-owned counter)
 */
typedef struct ClusterLmdSharedState {
	LWLock lwlock;				/* LWTRANCHE_CLUSTER_LMD guards non-atomic fields */
	pg_atomic_uint32 lmd_state; /* ClusterLmdState atomic (HC4 single ownership field) */
	pid_t pid;					/* set by LMD in STARTING */
	TimestampTz spawned_at;		/* set by LMD in STARTING */
	TimestampTz ready_at;		/* set by LMD in READY */
	TimestampTz stopped_at;		/* set by LMD in STOPPED */
	bool shutdown_requested;	/* postmaster sets; LMD main loop polls + exits */

	/* HC3 producer-side ConditionVariable wake substrate. */
	ConditionVariable cv;

	/*
	 * 6 counter (atomic) — spec-2.19 §0 Q8.  No work_queue_count placeholder
	 * (HC6 — v0.2 P1.2 / v0.3 P1.6 砍除 ring buffer).
	 */
	pg_atomic_uint64 lmd_started_count;
	pg_atomic_uint64 lmd_ready_at_us;
	pg_atomic_uint64 lmd_edge_submission_count;
	pg_atomic_uint64 lmd_wake_count;
	pg_atomic_uint64 lmd_idle_count;
	pg_atomic_uint64 lmd_error_count;
} ClusterLmdSharedState;


/*
 * Public API.
 */

/*
 * Postmaster spawn helper (thin proxy mirror of LMS).
 *
 *	Forwards to cluster_postmaster_start_lmd() which lives in
 *	postmaster.c.  Returns the LMD child pid on success, or 0 on
 *	spawn failure / lmd_enabled=off (DISABLED state at startup).
 *	Asserts !IsUnderPostmaster.
 */
extern int cluster_lmd_start(void);

/*
 * Postmaster sync wait for LMD readiness (bounded polling).
 *
 *	Returns true when state reaches READY within timeout_ms.  If
 *	lmd_enabled=off at startup, returns true immediately
 *	(DISABLED state is a legitimate "ready-or-skip" terminal).
 */
extern bool cluster_lmd_wait_for_ready(int timeout_ms);

/*
 * Postmaster shutdown signal.  Idempotent.  Asserts !IsUnderPostmaster.
 */
extern void cluster_lmd_request_shutdown(void);

/*
 * Postmaster reaper notification.  Non-blocking / LWLock-free by design:
 * the reaper must be able to clear a stale READY state even if the LMD
 * child died while holding its own LWLock.
 */
extern void cluster_lmd_mark_child_exit(void);

/*
 * LMD main entry — invoked from auxprocess.c dispatch.  Asserts
 * IsUnderPostmaster (reverse defense in depth).  Never returns.
 */
extern void LmdMain(void) pg_attribute_noreturn();

/*
 * Read-only accessors for SQL view + diagnostics.
 */
extern ClusterLmdState cluster_lmd_get_state(void);
extern pid_t cluster_lmd_get_pid(void);
extern TimestampTz cluster_lmd_get_spawned_at(void);
extern TimestampTz cluster_lmd_get_ready_at(void);
extern uint64 cluster_lmd_get_started_count(void);
extern uint64 cluster_lmd_get_edge_submission_count(void);
extern uint64 cluster_lmd_get_wake_count(void);
extern uint64 cluster_lmd_get_idle_count(void);
extern uint64 cluster_lmd_get_error_count(void);

/*
 * State enum -> canonical lowercase string ("disabled", "not_started",
 * "starting", "ready", "draining", "stopped").  Out-of-range returns
 * "(unknown)".  4-state view分流 maps to this output (HC2).
 */
extern const char *cluster_lmd_state_to_string(ClusterLmdState s);

/*
 * HC4 single ownership exact predicate.  spec-2.17 caller-side placeholder
 * + future spec-2.20+ wait-edge submitter call this to determine whether
 * LMD owns deadlock detection.  Returns true iff state == READY.
 *
 *	**禁止使用 `state >= LMD_READY` 数值比较** — DRAINING / STOPPED /
 *	DISABLED 不得误判为 READY (v0.3 codex P1.5 catch).  All caller-side
 *	ownership gates MUST go through this helper or compare exact
 *	== CLUSTER_LMD_READY.
 */
extern bool cluster_lmd_is_ready(void);

/*
 * HC3 producer wake — increments lmd_edge_submission_count atomically
 * and broadcasts cv.  Used by spec-2.17 caller-side placeholder when
 * gated by cluster_lmd_is_ready() (HC4).
 *
 *	HC6 skeleton:不保存 wait edge;调用即 atomic ++ submission_count +
 *	CV broadcast.  Real graph maintenance + Tarjan 推 spec-2.20+ 同 spec
 *	ship producer + consumer (L114 inherit).
 *
 *	No-op if LMD not initialized (cluster_lmd_state == NULL).
 */
extern void cluster_lmd_submit_wait_edge(void);

/*
 * spec-2.21 D7:cluster_lmd_cancel_wait_edge — 对称 stub counter ++.
 *
 *	S7 cleanup / FAIL_* 路径调用对称 cancel(no-op except atomic counter);
 *	real wait-edge removal 推 spec-2.22 LMD Tarjan(L107 forward-link)。
 *	无 LMD shmem 时 no-op safe(spec-2.19 shmem may be NULL early init)。
 */
extern void cluster_lmd_cancel_wait_edge(void);


/* ============================================================
 * spec-2.22 D1 — vertex identity + wait edge struct.
 *
 *	HC13 vertex identity = 4-tuple (node_id, procno, cluster_epoch,
 *	request_id).  Sort metadata (xid, local_start_ts_ms) is victim-
 *	selection only — xid may be InvalidTransactionId for advisory locks
 *	acquired before any write, autovacuum, parallel workers, etc.
 *
 *	HTAB index uses the complete 4-tuple.  Omitting cluster_epoch would
 *	allow stale procno/request_id reuse to overwrite or remove a live wait
 *	edge after backend slot reuse.
 * ============================================================ */

typedef struct ClusterLmdVertex {
	/* Identity 4-tuple (HC13). */
	int32 node_id;
	uint32 procno;
	uint64 cluster_epoch;
	uint64 request_id;

	/* Sort metadata (A4 victim selection only;not part of identity). */
	TransactionId xid; /* may be InvalidTransactionId — advisory lock OK */
	int64 local_start_ts_ms;
} ClusterLmdVertex;

StaticAssertDecl(sizeof(ClusterLmdVertex) == 40,
				 "ClusterLmdVertex ABI 40-byte lock (4+4+8+8 identity + 4+8 metadata + 4 pad)");

typedef struct ClusterLmdWaitEdge {
	ClusterLmdVertex waiter;  /* this backend stuck waiting on S4 */
	ClusterLmdVertex blocker; /* current holder waiter is blocked by */
	uint64 graph_generation;  /* snapshot at add time — revalidate basis */
	uint64 request_id;		  /* matches ClusterLockAcquireRequest.request_id */
} ClusterLmdWaitEdge;

StaticAssertDecl(sizeof(ClusterLmdWaitEdge) == 96,
				 "ClusterLmdWaitEdge ABI 96-byte lock (40 + 40 + 8 + 8)");


/*
 * spec-2.22 D1 — submit/cancel real API.
 *
 *	submit_wait_edge_real:  false return = wait edge table full;
 *	caller MUST go through S7 cleanup + ereport 53R82 (HC12 fail-closed;
 *	severely禁止 fallback PG local deadlock detector).
 */
extern bool cluster_lmd_submit_wait_edge_real(const ClusterLmdVertex *waiter,
											  const ClusterLmdVertex *blocker, uint64 request_id);

extern void cluster_lmd_cancel_wait_edge_real(const ClusterLmdVertex *waiter);

/*
 * Manual SQL trigger for Tarjan scan (admin / TAP test hook).
 *	Runs one local scan + revalidate cycle synchronously.
 */
extern void cluster_lmd_run_tarjan_scan_now(void);

/* Graph generation accessor — bumped monotonically on add/remove. */
extern uint64 cluster_lmd_graph_generation_get(void);

/* Counter accessors — populated by D4 graph + D3 tarjan. */
extern uint64 cluster_lmd_wait_edge_count_get(void);
extern uint64 cluster_lmd_wait_edge_full_count_get(void);
extern uint64 cluster_lmd_tarjan_scan_count_get(void);
extern uint64 cluster_lmd_cycle_detected_count_get(void);
extern uint64 cluster_lmd_victim_cancel_sent_count_get(void);
extern uint64 cluster_lmd_revalidate_fail_count_get(void);
extern uint64 cluster_lmd_cross_node_victim_pending_count_get(void);
extern uint64 cluster_lmd_inject_call_count_get(void);

/* spec-2.23 D8 — coordinator probe counters. */
extern uint64 cluster_lmd_probe_broadcast_count_get(void);
extern uint64 cluster_lmd_probe_partial_count_get(void);

/* Counter increments — LMD graph shmem-owned, called by Tarjan worker. */
extern void cluster_lmd_tarjan_scan_count_inc(uint64 delta);
extern void cluster_lmd_cycle_detected_count_inc(uint64 delta);
extern void cluster_lmd_victim_cancel_sent_count_inc(uint64 delta);
extern void cluster_lmd_revalidate_fail_count_inc(uint64 delta);
extern void cluster_lmd_cross_node_victim_pending_count_inc(uint64 delta);
extern void cluster_lmd_probe_broadcast_count_inc(uint64 delta);
extern void cluster_lmd_probe_partial_count_inc(uint64 delta);

/* spec-2.23 D8 — coordinator scan + REPORT collector entry points. */
struct GesDeadlockReportHeader;
extern void cluster_lmd_tarjan_run_coordinator_scan(int collect_timeout_ms);
extern bool cluster_lmd_probe_collect_receive(const struct GesDeadlockReportHeader *report,
											  Size report_len);

/* ============================================================
 * spec-2.24 D2 — LMD-owned cancel queue (HC23).
 *
 *	Receives GES_REQ_OPCODE_CANCEL_PENDING items from cluster_ges
 *	request_handler.  LMD daemon drains, performs HC24 4-tuple stale
 *	procno match, and dispatches to cluster_lmd_signal_local_victim.
 *
 *	Bounded ring (CLUSTER_LMD_CANCEL_QUEUE_DEPTH = 256).  Enqueue full
 *	returns false; handler increments cross_node_cancel_queue_full_count.
 *	Independent from cluster_grd_work_queue (which is owned by GES grant
 *	consumer LMS — never double-consumed per spec-2.18 Q2 boundary).
 * ============================================================ */

#define CLUSTER_LMD_CANCEL_QUEUE_DEPTH 256

typedef struct ClusterLmdCancelItem {
	uint32 source_node_id;	  /* sender of GES_REQ_OPCODE_CANCEL_PENDING */
	uint16 payload_len;		  /* sizeof(GesRequestPayload) = 48 */
	uint16 _reserved;
	uint8 payload[48];		  /* GesRequestPayload byte-image */
} ClusterLmdCancelItem;

StaticAssertDecl(sizeof(ClusterLmdCancelItem) == 56, "ClusterLmdCancelItem 56-byte lock");

extern Size cluster_lmd_cancel_queue_shmem_size(void);
extern void cluster_lmd_cancel_queue_shmem_init(void);

extern bool cluster_lmd_cancel_queue_enqueue(uint32 source_node_id, const void *payload,
											 uint16 payload_len);
extern bool cluster_lmd_cancel_queue_dequeue(ClusterLmdCancelItem *out);
extern void cluster_lmd_drain_cancel_queue(void);

/* spec-2.24 D8 — LMD periodic dead-backend cleanup sweep (HC28). */
extern void cluster_lmd_run_periodic_cleanup_sweep(void);

/* spec-2.24 D12 — NEW counters. */
extern uint64 cluster_lmd_cross_node_victim_cancel_sent_count_get(void);
extern uint64 cluster_lmd_cross_node_cancel_received_count_get(void);
extern uint64 cluster_lmd_cross_node_cancel_queue_full_count_get(void);
extern uint64 cluster_lmd_cleanup_on_backend_exit_count_get(void);
extern uint64 cluster_lmd_cleanup_lmd_sweep_count_get(void);
extern uint64 cluster_lmd_cleanup_skip_other_owner_count_get(void);
extern void cluster_lmd_cross_node_victim_cancel_sent_count_inc(uint64 delta);
extern void cluster_lmd_cross_node_cancel_received_count_inc(uint64 delta);
extern void cluster_lmd_cross_node_cancel_queue_full_count_inc(uint64 delta);
extern void cluster_lmd_cleanup_on_backend_exit_count_inc(uint64 delta);
extern void cluster_lmd_cleanup_lmd_sweep_count_inc(uint64 delta);
extern void cluster_lmd_cleanup_skip_other_owner_count_inc(uint64 delta);

/* D16 test-only injection helper (also surfaced via SQL SRF). */
extern bool cluster_lmd_inject_wait_edge(const ClusterLmdVertex *waiter,
										 const ClusterLmdVertex *blocker);

extern void cluster_lmd_inject_call_count_inc(void);

/* spec-2.22 D4/D5 — graph shmem region API (registered via spec-1.3 registry). */
extern Size cluster_lmd_graph_shmem_size(void);
extern void cluster_lmd_graph_shmem_init(void);

/* Graph low-level mutators (used by cluster_lmd_tarjan.c + D16 SRF). */
extern bool cluster_lmd_graph_add_edge(const ClusterLmdWaitEdge *edge);
extern bool cluster_lmd_graph_remove_edge_by_waiter(const ClusterLmdVertex *waiter);
extern int cluster_lmd_graph_snapshot_copy(ClusterLmdWaitEdge *out_buf, int max_edges,
										   uint64 *out_gen_at_snapshot);

/* spec-2.22 D3 — Tarjan public API. */
extern int cluster_lmd_tarjan_scan_snapshot(const ClusterLmdWaitEdge *edges, int nedges,
											ClusterLmdVertex *out_cycle_vertices,
											int max_cycle_vertices, int *out_cycle_count);
extern void cluster_lmd_tarjan_pick_victim(const ClusterLmdVertex *cycle_vertices, int nvertices,
										   ClusterLmdVertex *out_victim);
extern bool cluster_lmd_tarjan_revalidate(const ClusterLmdVertex *cycle_vertices, int nvertices,
										  uint64 snapshot_generation);
extern void cluster_lmd_tarjan_run_local_scan(void);

/* spec-2.22 D8 — victim signal helper (defined in cluster_lmd_tarjan.c
 * Step 6;forward-declared so Step 4 build links). */
extern void cluster_lmd_signal_local_victim(uint32 procno, uint64 request_id, uint64 cluster_epoch);

/*
 * shmem region helpers — registered by cluster_init_shmem_module()
 * via the spec-1.3 region registry.
 */
extern Size cluster_lmd_shmem_size(void);
extern void cluster_lmd_shmem_request(void);
extern void cluster_lmd_shmem_init(void);
extern void cluster_lmd_shmem_register(void);
extern ClusterLmdSharedState *cluster_lmd_shared_state(void);

#endif /* CLUSTER_LMD_H */
