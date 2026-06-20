/*-------------------------------------------------------------------------
 *
 * cluster_lock_acquire.h
 *	  pgrac 7-step state machine caller-side internal API — spec-2.20
 *	  Sprint A skeleton (descoped scope per v0.4 — internal API only;
 *	  PG LockAcquireExtended integration 推 spec-2.21).
 *
 *	  Spec-2.20 ships LMS + 7-step **internal** production wire:
 *	  cluster_lock_acquire S1-S7 internal API + LMS grant decision body
 *	  + unit tests.  **不碰** PG LockAcquireExtended hot path（推 spec-2.21
 *	  PG hook integration ship 时 wire to lock.c）+ **不做** LMD Tarjan
 *	  （推 spec-2.22 LMD Tarjan + cross-node deadlock detection ship）。
 *
 *	  spec-2.17 ship 了 caller-side 4-node placeholder（S1-S7 wire 点）+
 *	  spec-2.18 ship LMS daemon skeleton + spec-2.19 ship LMD daemon
 *	  skeleton.  spec-2.20 把 placeholder 内部填实 S1-S7 函数 body（不
 *	  接入 PG hot path），让 cluster_unit 可以直接调 S1-S7 verify
 *	  invariants（HC4 exact predicate / I2 reservation-before-LockAcquire /
 *	  S3 sub-step 顺序硬契约）。
 *
 *	  HC1 LMS unavailable fail-closed:S1 entry 检查
 *	  cluster_lms_is_ready() exact == LMS_READY;非 READY + enabled=on →
 *	  返回 53R80 ERRCODE_CLUSTER_LMS_UNAVAILABLE caller retry/rollback。
 *
 *	  HC4 exact predicate(spec-2.19 L124 inherit):每 S1 entry 必 exact
 *	  predicate(`state == LMS_READY`);禁止 `>= LMS_READY` 数值比较。
 *
 *	  spec-2.17 §1.4 Q6 F3 race window 防御(I2 hard contract):S3
 *	  sub-step 顺序必走全:
 *	    S3.1 acquire shard partition LWLock + check holders[]
 *	    S3.2 insert PROCLOCK reservation slot
 *	    S3.3 release partition LWLock
 *	    S3.4 PG LockAcquire local（spec-2.21 wire — 本 spec internal API
 *	         返回 PENDING）
 *	    S3.5 acquire partition LWLock + promote reservation to real holder
 *	    S3.6 release partition LWLock
 *
 *	  失败 rollback intent + remove reservation 走 S7 cleanup。
 *
 *	  Local-master fast path(A1 spec-2.20 v0.3 architectural):
 *	    caller under GRD partition LWLock 判定 resource local-only
 *	    (no remote holder/waiter/wait edge) → 直接 PG-native LockAcquire
 *	    路径 + promote local holder;**不入 LMS work_queue**。
 *	    spec-2.21 wire 时 in lock.c hook 内实现 fast path;本 spec
 *	    internal API 仅声明边界。
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_lock_acquire.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-2.20-7step-state-machine-activation.md(v0.4 descoped
 *	  scope:LMS + 7-step internal production wire only;PG hook 推
 *	  spec-2.21;LMD Tarjan 推 spec-2.22)。
 *	  Anchor: spec-2.17 caller-side 4-node placeholder scaffolding(S1-S7
 *	  wire 点)+ spec-2.18 LMS daemon API + spec-2.19 LMD daemon API。
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_LOCK_ACQUIRE_H
#define CLUSTER_LOCK_ACQUIRE_H

#include "access/transam.h"		  /* FirstNormalObjectId for HC24/HC27 */
#include "cluster/cluster_conf.h" /* CLUSTER_MAX_NODES for HC47 */
#include "cluster/cluster_grd.h"  /* ClusterResId + ClusterGrdHolderId */
#include "cluster/cluster_guc.h"  /* cluster_node_id extern for HC47 */
#include "storage/lock.h"		  /* LOCKMODE / LOCKTAG / ShareUpdateExclusiveLock */


/*
 * ClusterLockAcquireResult — 7-step state machine final outcome.
 *
 *	S1-S7 任一 step 失败走 S7 cleanup 返回 _FAIL_* 类;成功完整流程返回
 *	OK_GRANTED 或 OK_CONVERTED(原 grant + new mode);PENDING 表示 S4
 *	异步等(spec-2.21 hot path integration 时 caller 走 WaitLatch 等
 *	GES_REPLY)。
 */
typedef enum ClusterLockAcquireResult {
	CLUSTER_LOCK_ACQUIRE_OK_GRANTED = 0,   /* S5 promote success(new holder) */
	CLUSTER_LOCK_ACQUIRE_OK_CONVERTED = 1, /* S5 promote success(mode convert) */
	CLUSTER_LOCK_ACQUIRE_PENDING = 2,	   /* S4 async wait — caller waits GES_REPLY */
	CLUSTER_LOCK_ACQUIRE_OK_NATIVE = 3,	   /* cluster gate disabled; caller uses PG-native path */
	CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK
	= 4, /* spec-2.21:S1-S4 reservation/grant 完成,caller(lock.c)调 PG-native LockAcquire + S5 promote */
	CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE = 10,	 /* S1 53R80 fail-closed */
	CLUSTER_LOCK_ACQUIRE_FAIL_GRD_NOT_READY = 11,	 /* S2 cluster.grd_max_entries=0 */
	CLUSTER_LOCK_ACQUIRE_FAIL_RESERVATION_FULL = 12, /* S3 GRD entry full / 53R71 */
	CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT = 13,			 /* S4 53R70 cluster_ges_timeout */
	CLUSTER_LOCK_ACQUIRE_FAIL_DEADLOCK = 14, /* S4 53R72 cluster_ges_deadlock(spec-2.22 真激活)*/
	CLUSTER_LOCK_ACQUIRE_FAIL_CANCEL = 15,	 /* S4 53R73 cluster_ges_cancel_pending */
	CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL = 16, /* S5/S6/S7 internal error */
	CLUSTER_LOCK_ACQUIRE_FAIL_LMD_WAIT_EDGE_FULL
	= 17, /* spec-2.22 D7: LMD wait-edge cap exhausted; caller maps to 53R82 fail-closed */
	/* spec-4.6 D4 — failure-driven remaster request surfaces. */
	CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING
	= 18, /* shard FROZEN/REBUILDING past wait budget → 53R9I retry */
	CLUSTER_LOCK_ACQUIRE_FAIL_STALE_GENERATION
	= 19, /* stale epoch/master generation on the wire → 53R9J retry */
	/* spec-5.1b D3 — master rejected an unsupported request (currently the
	 * cross-node opcode-2 convert path); caller maps to
	 * ERRCODE_FEATURE_NOT_SUPPORTED (0A000).  The real backend convert
	 * trigger lands in spec-5.2; this surface is the requester side of the
	 * GES_REJECT_REASON_FEATURE_NOT_SUPPORTED reply. */
	CLUSTER_LOCK_ACQUIRE_FAIL_FEATURE_NOT_SUPPORTED = 20,
	/* spec-5.3 D4 — convert from→to is not a valid partial-order upgrade
	 * (LATERAL / no matching holder); caller maps to
	 * ERRCODE_CLUSTER_GES_ILLEGAL_LOCK_CONVERSION (53R74). */
	CLUSTER_LOCK_ACQUIRE_FAIL_ILLEGAL_CONVERT = 21
} ClusterLockAcquireResult;


/*
 * ClusterLockAcquireRequest — S1-S7 caller-side context.
 *
 *	spec-2.21 hot path integration 时 lock.c hook 填充此 struct + 调
 *	cluster_lock_acquire_seven_step()。spec-2.20 internal API 让
 *	cluster_unit 直接构造 + 验 S1-S7 invariants。
 *
 *	HC4 exact predicate:caller 必走 cluster_lms_is_ready() helper +
 *	cluster_lmd_is_ready() helper;禁止 `state >= READY` 数值比较。
 */
/*
 * spec-5.3 D1 — REQUEST vs CONVERT operation for the cluster acquire path.
 *
 *	A same-backend upgrade of an already-held cluster-registered lock
 *	(e.g. LOCK TABLE t IN SHARE MODE then ALTER TABLE t, which wants
 *	AccessExclusive) is an Oracle DLM-style lock conversion rather than a
 *	second additive grant.  cluster_lock_decide_op() picks CONVERT when a
 *	strictly-weaker cluster_registered LOCALLOCK already exists for the same
 *	locktag (and cluster.tm_convert_mode = 'convert'); otherwise REQUEST.
 */
typedef enum ClusterLockOp {
	CLUSTER_LOCK_OP_REQUEST = 1,
	CLUSTER_LOCK_OP_CONVERT = 2
} ClusterLockOp;

typedef struct ClusterLockAcquireRequest {
	/* 16B canonical wire-encoded ResId (spec-2.14 ship). */
	ClusterResId resid;
	/* spec-2.21 D1: caller PG LOCKTAG for gate predicate + identity. */
	LOCKTAG locktag;
	/* requested PG lock mode */
	LOCKMODE lockmode;
	/* spec-5.3 D1: REQUEST (default) or CONVERT (same-backend upgrade). */
	ClusterLockOp op;
	/* spec-5.3 D1: for CONVERT, the strongest already-held cluster-registered
	 * mode on this locktag — the REDECLARE locator key sent to the master. */
	LOCKMODE current_mode;
	/* DEFAULT_LOCKMETHOD / SHORT_LOCKMETHOD / cluster-aware class */
	int lockmethod_id;
	/* true → S4 immediate ConditionalLock semantic (no wait) */
	bool dontwait;
	/* spec-2.21 D1: HC11 session advisory stays native (skip cluster path). */
	bool sessionLock;
	/* spec-2.21 D1: S3 reservation pin / S5 promote / S6 release identity. */
	ClusterGrdHolderId holder;
	/* spec-2.21 D1: per-acquire monotonic id; LOCALLOCK exactly-once key. */
	uint64 request_id;
	/* spec-2.21 P2.3: S3 snapshot; S5 revalidate fail → backout. */
	uint64 master_gen_snapshot;
	/* spec-2.17 P2.2 deterministic 4-tuple — DESC = newer = youngest victim. */
	uint64 caller_local_start_ts_ms;
} ClusterLockAcquireRequest;

/*
 * ClusterLockReleaseRequest — spec-2.21 D1:cluster_lock_release() input.
 *
 *	D2/D3 LockRelease / LockReleaseAll / ResourceOwnerReleaseInternal hook
 *	填充此 struct + 调 cluster_lock_release()。LOCALLOCK->cluster_registered
 *	gate exactly-once 调用(HC9 grant/release 对称契约)。
 */
typedef struct ClusterLockReleaseRequest {
	LOCKTAG locktag; /* identity */
	LOCKMODE lockmode;
	bool sessionLock;
	ClusterGrdHolderId holder; /* from LOCALLOCK->cluster_holder */
	uint64 request_id;		   /* from LOCALLOCK->cluster_request_id */
	bool cluster_registered;   /* from LOCALLOCK->cluster_registered */
} ClusterLockReleaseRequest;


/*
 * Public API — 7-step state machine entry point.
 *
 *	spec-2.21 hot path:lock.c hook 调此函数 — internal dispatch S1-S7。
 *	spec-2.20 internal-only:cluster_unit T-7step 直接调 verify 各 step
 *	invariant(HC4 exact / I2 reservation-before-LockAcquire / S3
 *	sub-step 顺序 / 错误路径走 S7 cleanup)。
 *
 *	Returns: ClusterLockAcquireResult.  Caller-side error handling per
 *	5 SQLSTATE family(53R70 timeout / 53R71 pending_full / 53R72
 *	deadlock / 53R73 cancel / 53R80 LMS_UNAVAILABLE / 53R81
 *	LMD_UNAVAILABLE — spec-2.22 wire)。
 */
extern ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *req);


/*
 * spec-5.3 D1 — decide REQUEST vs CONVERT for the cluster acquire path.
 *
 *	REQUESTER-LOCAL detection: scans this backend's own LockMethodLocalHash
 *	for a cluster_registered LOCALLOCK on the SAME locktag whose mode is
 *	strictly weaker than req->lockmode.  This is topology-agnostic (works for
 *	local- AND remote-master) — the requester cannot read a remote master's
 *	GRD holders[].  Returns CLUSTER_LOCK_OP_CONVERT iff such a weaker hold
 *	exists AND cluster.tm_convert_mode = 'convert'; *current_mode_out is the
 *	strongest already-held cluster-registered mode (the locator key), and
 *	*weaker_locallock_out is that LOCALLOCK (so the caller can de-register it
 *	on a successful convert — §3.1a release-ownership).  Otherwise returns
 *	CLUSTER_LOCK_OP_REQUEST.
 */
extern ClusterLockOp cluster_lock_decide_op(const ClusterLockAcquireRequest *req,
											LOCKMODE *current_mode_out,
											LOCALLOCK **weaker_locallock_out);


/*
 * S1-S7 individual step API — internal-only.
 *
 *	cluster_unit 直接调每 step verify invariants;
 *	cluster_lock_acquire_seven_step() 内部按顺序 dispatch。spec-2.21
 *	hot path integration 时不直接调 individual step,只调
 *	cluster_lock_acquire_seven_step() top-level entry。
 */

/*
 * S1 entry:HC1 fail-closed check(cluster_lms_is_ready() exact == READY
 *	+ cluster_lmd_is_ready() 视 spec-2.22 wire);LMS_UNAVAILABLE → 53R80;
 *	LMD_UNAVAILABLE → 53R81(spec-2.22 wire)。
 */
extern ClusterLockAcquireResult cluster_lock_acquire_s1_entry(const ClusterLockAcquireRequest *req);

/*
 * S2 identity:resolve ClusterResId valid + lockmethod_id is cluster-aware
 *	class(spec-2.14 4 class scaffolding;real expansion spec-2.25)。
 */
extern ClusterLockAcquireResult
cluster_lock_acquire_s2_identity(const ClusterLockAcquireRequest *req);

/*
 * S3 partition + reservation:S3.1-S3.6 sub-step sequence(spec-2.17
 *	§1.4 Q6 F3 race window 防御 — 所有路径必走全;fast path 不绕过)。
 *	S3.4 PG LockAcquire local 在 spec-2.20 internal API 返回 PENDING
 *	(spec-2.21 hot path wire)。
 */
extern ClusterLockAcquireResult
cluster_lock_acquire_s3_partition_reservation(const ClusterLockAcquireRequest *req);

/*
 * S4 remote request + wait:GES_REQUEST send + WAIT_EVENT_CLUSTER_GES_S4_WAIT
 *	+ GES_REPLY 等(timeout 53R70 / cancel 53R73 / deadlock 53R72 via
 *	LMD spec-2.22 wire)。local-master + no remote holder fast path 不
 *	进 S4(A1)。
 */
extern ClusterLockAcquireResult
cluster_lock_acquire_s4_remote_request_wait(const ClusterLockAcquireRequest *req);

/*
 * S5 promote holder:acquire partition LWLock + promote reservation to
 *	real holder + release partition。spec-2.21 hot path wire 时此 step
 *	post PG LockAcquire success 调。
 */
extern ClusterLockAcquireResult
cluster_lock_acquire_s5_promote_holder(const ClusterLockAcquireRequest *req);

/*
 * S6 release:backend done 释放路径(LockRelease hook;spec-2.21 wire 到
 *	PG LockRelease)。
 */
extern ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req);

/*
 * S7 cleanup:error/timeout 路径 rollback intent + remove reservation +
 *	send GES_CANCEL_PENDING / GES_RELEASE 视已 grant 状态(spec-2.17
 *	§1.4 P1.4 不同 opcode 分流硬契约)。
 */
extern ClusterLockAcquireResult
cluster_lock_acquire_s7_cleanup(const ClusterLockAcquireRequest *req);


/*
 * spec-2.21 D1 + spec-2.25 D1:cluster_lock_should_globalize — PG hot-path
 *	gate predicate.  Returns true iff this lock should enter the cluster
 *	7-step state machine instead of PG-native LockAcquire only.
 *
 *	spec-2.21 v0.3 frozen:仅 transaction-level LOCKTAG_ADVISORY。
 *	spec-2.25 v0.2 frozen:扩展到 LOCKTAG_RELATION + LOCKTAG_OBJECT,
 *	  HC23 lockmode >= ShareUpdateExclusiveLock(5) + HC24/HC27
 *	  oid >= FirstNormalObjectId + HC25 relpersistence != 't'(temp skip)。
 *
 *	预期 cache-hot inline path:ADVISORY 命中 ≤ 2 ns;低模式 RELATION
 *	(SELECT/INSERT/UPDATE/DELETE)在 lockmode < SUEX 分支 ≤ 3 ns 短路;
 *	高模式 RELATION 命中 syscache 路径 ~50 ns(仅 DDL — 不在 OLTP hot path)。
 */
extern bool cluster_relation_is_persistent_or_unlogged(Oid relid);

static inline bool
cluster_lock_should_globalize(const LOCKTAG *locktag, LOCKMODE lockmode, bool sessionLock)
{
	if (locktag == NULL)
		return false;
	if (sessionLock)
		return false; /* HC11: session advisory stays native */

	switch (locktag->locktag_type) {
	case LOCKTAG_ADVISORY:
		return true; /* spec-2.21 ship — no mode filter */

	case LOCKTAG_RELATION:
		/* HC23 OLTP fast-path:  AccessShare / RowShare / RowExclusive go
			 * PG-native.  ShareUpdateExclusiveLock (=5) is the lowest mode
			 * routed through cluster. */
		if (lockmode < ShareUpdateExclusiveLock)
			return false;
		/* HC24 system-catalog bootstrap-safe:  pg_class etc oids
			 * < FirstNormalObjectId never enter cluster gate. */
		if (locktag->locktag_field2 < FirstNormalObjectId)
			return false;
		/* HC25 relpersistence skip:  temp tables go PG-native;
			 * unlogged + permanent route through cluster. */
		return cluster_relation_is_persistent_or_unlogged(locktag->locktag_field2);

	case LOCKTAG_OBJECT:
		/* HC26 mirror HC23 — only >= SUEX modes go cluster. */
		if (lockmode < ShareUpdateExclusiveLock)
			return false;
		/* HC27 objoid >= FirstNormal — classoid is always system catalog
			 * oid (pg_proc / pg_type etc).  Filter on objoid (field3). */
		if (locktag->locktag_field3 < FirstNormalObjectId)
			return false;
		return true;

	case LOCKTAG_TRANSACTION:
		/* spec-2.26 NEW — HC39 exact mode filter.
		 *
		 *	PG XactLockTable* paths use only ShareLock (5 — waiter via
		 *	XactLockTableWait) and ExclusiveLock (7 — owner via
		 *	XactLockTableInsert).  All other modes are defensively
		 *	rejected.  Do NOT use `lockmode >= ShareLock` because that
		 *	would unnecessarily admit ShareRowExclusive (6) /
		 *	AccessExclusive (8) which PG never takes on TRANSACTION
		 *	locktag (and were misordered as ShareLock=4 in v0.1).
		 *
		 *	HC47 invalid cluster_node_id fail-closed — at startup /
		 *	single-node configurations cluster_node_id may still be -1
		 *	(uninitialized).  Casting (-1) to uint32 yields 0xFFFFFFFF
		 *	which corrupts ClusterResId.field2 on the wire (silent
		 *	wrong-master).  Must check range before encoding identity.
		 */
		if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
			return false; /* HC47 fail-closed: invalid origin node id */
		return lockmode == ShareLock || lockmode == ExclusiveLock;

	default:
		/* HC28 — PAGE / TUPLE / RELATION_EXTEND / VIRTUALTRANSACTION /
			 * SPECULATIVE_TOKEN / APPLY_TRANSACTION / USERLOCK 永远 PG-native;
			 * AD-012 例外 10 完整 vision(allocator subspace + visibility
			 * fork + pg_xact remote)推 spec-2.27+ / Cache Fusion(spec-2.28+). */
		return false;
	}
}


/*
 * spec-2.21 D1:cluster_lock_release — normal release path entry.
 *
 *	D2 LockRelease / LockReleaseAll + D3 ResourceOwnerReleaseInternal(LOCKS)
 *	hook 调此函数;LOCALLOCK->cluster_registered=true 且 nLocks==1 时调用,
 *	exactly-once 契约 HC9。
 */
extern void cluster_lock_release(const ClusterLockReleaseRequest *req);


/*
 * spec-2.21 D4 P2.3:S5 promote with revalidate;失败 5-step backout 序列
 *	(cancel reservation + ereport caller responsibility)。
 *
 *	master_gen_snapshot:S3 acquire 时 snapshot(写入 req->master_gen_snapshot)。
 */
extern ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *req);


/*
 * Read-only accessor — 7-step S1-S7 path counters(diagnostics only;
 *	不预设 production-granularity per spec-2.20 v0.4 frozen scope)。
 */
extern uint64 cluster_lock_acquire_s1_entry_count(void);
extern uint64 cluster_lock_acquire_s7_cleanup_count(void);

/*
 * spec-2.21 D4 P2.4:NEW dump_cluster_lock_acquire — 6 emit_row(s1_entry /
 *	s3_reservation / s4_remote / s5_promote / s6_release / s7_cleanup)
 *	供 030_acceptance L18 HC9 grant-release 对称 acceptance gate。
 */
extern void cluster_lock_acquire_dump(void (*emit_row)(void *cookie, const char *key,
													   const char *value),
									  void *cookie);

#endif /* CLUSTER_LOCK_ACQUIRE_H */
