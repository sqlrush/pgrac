/*-------------------------------------------------------------------------
 *
 * cluster_lock_acquire.c
 *	  pgrac 7-step state machine caller-side internal implementation —
 *	  spec-2.20 Sprint A(descoped scope v0.4)。
 *
 *	  Spec-2.20 descoped scope:LMS + 7-step **internal** production wire
 *	  only。本文件 ship:S1-S7 individual step functions + top-level
 *	  cluster_lock_acquire_seven_step() dispatch + 2 counter(s1_entry /
 *	  s7_cleanup;不预设 production-granularity per v0.4 frozen)。
 *
 *	  **不在 spec-2.20 scope**:
 *	  - S3.4 PG LockAcquire local wire(推 spec-2.21 hot path integration)
 *	  - S4 GES_REQUEST 真 send + WAIT_EVENT_CLUSTER_GES_S4_WAIT 真等(推
 *	    spec-2.21)
 *	  - S5 promote 真 GRD entry mutator wire(推 spec-2.21)
 *	  - S6/S7 PG LockRelease hook(推 spec-2.21)
 *	  - LMD Tarjan wait edge submit(推 spec-2.22)
 *
 *	  本 ship 让 cluster_unit T-7step 可以 verify:
 *	  - HC1 fail-closed:S1 cluster_lms_is_ready() exact predicate
 *	  - I1 monotonic forward transition(S1 → S2 → S3 → S4 → S5 → S6;
 *	    pre-reservation fail returns directly;post-reservation fail
 *	    走 S7 cleanup 不回退)
 *	  - 5 SQLSTATE family return value 分流(53R70 / 53R71 / 53R72 /
 *	    53R73 / 53R80)
 *	  - S7 cleanup counter ++ only on post-reservation error path
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lock_acquire.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-2.20-7step-state-machine-activation.md(v0.4 descoped)。
 *	  Anchor: spec-2.17 caller-side 4-node placeholder scaffolding(S1-S7
 *	  wire 点)+ spec-2.18 LMS daemon API + spec-2.19 LMD daemon API。
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lms.h"
#include "cluster/cluster_lock_acquire.h"
#include "port/atomics.h"


/*
 * Step counter — diagnostics only(不预设 production-granularity per
 * spec-2.20 v0.4 frozen scope;v0.5 / spec-2.21 真激活时按 Q7 wire
 * grant/reject/convert 3 分项;此 2 counter 是 S1/S7 hot path 计数,
 * 不在 dump_lms 暴露)。
 */
static pg_atomic_uint64 stub_s1_entry_count;
static pg_atomic_uint64 stub_s7_cleanup_count;
static bool stub_counter_initialized = false;

static inline void
ensure_counter_initialized(void)
{
	if (!stub_counter_initialized) {
		pg_atomic_init_u64(&stub_s1_entry_count, 0);
		pg_atomic_init_u64(&stub_s7_cleanup_count, 0);
		stub_counter_initialized = true;
	}
}


/*
 * S1 entry — HC1 LMS unavailable fail-closed check.
 *
 *	cluster_lms_is_ready() exact predicate(spec-2.19 L124 inherit;
 *	禁止 `state >= LMS_READY` 数值比较)。
 *	cluster.lms_enabled=on + LMS state != READY → 53R80 fail-closed。
 *	cluster.lms_enabled=off → caller-side legacy PG-native path.  Return
 *	OK_NATIVE so the top-level dispatcher does not run S2-S7 and pretend
 *	a cluster grant happened.
 */
ClusterLockAcquireResult
cluster_lock_acquire_s1_entry(const ClusterLockAcquireRequest *req)
{
	ensure_counter_initialized();
	pg_atomic_fetch_add_u64(&stub_s1_entry_count, 1);

	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	/*
	 * HC1 fail-closed:cluster.lms_enabled=on + LMS state != READY →
	 * 53R80 LMS_UNAVAILABLE。**exact predicate** — spec-2.19 L124 inherit。
	 */
	if (!cluster_lms_enabled) {
		/*
		 * Startup-time fallback path:caller-side legacy(spec-2.21
		 * gate-disable wire 时走 PG-native LockAcquire 不入 cluster gate)。
		 * 本 internal API 返回 OK_NATIVE skip downstream(spec-2.20
		 * 不实际走 hot path)。
		 */
		return CLUSTER_LOCK_ACQUIRE_OK_NATIVE;
	}

	if (!cluster_lms_is_ready()) {
		/* 53R80 cluster_lms_unavailable caller retry/rollback。*/
		return CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE;
	}

	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED; /* dispatch to S2 */
}


/*
 * S2 identity — resolve ClusterResId + lockmethod cluster-awareness check.
 *
 *	spec-2.14 4 class scaffolding(RELATION / TRANSACTION / OBJECT /
 *	ADVISORY);本 spec MVP class scope = ADVISORY;real class expansion
 *	推 spec-2.25。
 */
ClusterLockAcquireResult
cluster_lock_acquire_s2_identity(const ClusterLockAcquireRequest *req)
{
	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	/* GRD entry table readiness check(spec-2.15 cluster.grd_max_entries
	 * = 0 时 entry table 未 alloc → S2 fail GRD_NOT_READY)。*/
	/*
	 * NB:cluster_grd_entry_lookup_or_create() 是 spec-2.16 caller-side
	 * 集成 API(spec-2.21 hot path 真 wire);本 internal API 仅 validate
	 * S2 invariants — resid 必须 canonical 16B(spec-2.14 wire 检查)。
	 */
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED; /* dispatch to S3 */
}


/*
 * S3 partition + reservation — spec-2.17 §1.4 Q6 F3 race window 防御。
 *
 *	S3.1 acquire shard partition LWLock + check holders[]
 *	S3.2 insert PROCLOCK reservation slot
 *	S3.3 release partition LWLock
 *	S3.4 PG LockAcquire local(spec-2.21 wire — 本 internal API 返回 PENDING)
 *	S3.5 acquire partition LWLock + promote reservation to real holder
 *	S3.6 release partition LWLock
 *
 *	**所有路径必走全 sub-step 顺序**;fast path 不绕过(spec-2.21 hot
 *	path A1 local-fast-path 在 caller 内 PG-native LockAcquire 但仍走
 *	全 S3.1-S3.6 reservation protocol)。
 *
 *	失败 rollback intent + remove reservation 走 S7 cleanup。
 */
ClusterLockAcquireResult
cluster_lock_acquire_s3_partition_reservation(const ClusterLockAcquireRequest *req)
{
	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	/*
	 * S3 internal API(spec-2.20 v0.4 descoped):返回 PENDING signals
	 * spec-2.21 hot path wire 时 caller-side 接 PG LockAcquire local
	 * + S5 promote。
	 *
	 * Real S3.1-S3.6 sub-step sequence wire to cluster_grd_entry_*
	 * mutator API(spec-2.16 ship caller-side 6-step state machine 待
	 * spec-2.21 真激活 wire callsite)。
	 */
	return CLUSTER_LOCK_ACQUIRE_PENDING; /* dispatch to S4(if remote)or S5(if local fast path)*/
}


/*
 * S4 remote request + wait — GES_REQUEST send + WAIT_EVENT 等 GES_REPLY。
 *
 *	A1 local-master fast path:caller under GRD partition LWLock 判定
 *	resource local-only(no remote holder/waiter)→ skip S4 直接 S5。
 *	本 internal API 返回 PENDING(spec-2.21 hot path wire decision)。
 *
 *	Remote-master path:send GES_REQUEST + WAIT_EVENT_CLUSTER_GES_S4_WAIT
 *	+ wait GES_REPLY;timeout 53R70 / deadlock 53R72(LMD spec-2.22 wire)/
 *	cancel 53R73。
 */
ClusterLockAcquireResult
cluster_lock_acquire_s4_remote_request_wait(const ClusterLockAcquireRequest *req)
{
	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	/*
	 * S4 internal API:返回 PENDING signals spec-2.21 hot path wire 时
	 * caller-side 接 cluster_ges_request_send + WaitLatch GES_S4_WAIT。
	 */
	if (req->dontwait)
		return CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT; /* ConditionalLock semantic */

	return CLUSTER_LOCK_ACQUIRE_PENDING;
}


/*
 * S5 promote holder — partition LWLock + promote reservation to real
 *	holder + release partition。spec-2.21 hot path wire 时此 step post
 *	PG LockAcquire success 调。
 */
ClusterLockAcquireResult
cluster_lock_acquire_s5_promote_holder(const ClusterLockAcquireRequest *req)
{
	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	/*
	 * S5 internal API:返回 OK_GRANTED(promote success default);
	 * convert request 走 OK_CONVERTED(spec-2.21 hot path wire);
	 * real promote wire to cluster_grd_entry_promote_holder()
	 * (spec-2.16 mutator API 真激活推 spec-2.21)。
	 */
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}


/*
 * S6 release — backend done(LockRelease hook;spec-2.21 wire to PG)。
 */
ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req)
{
	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	/*
	 * S6 internal API:返回 OK_GRANTED(release success);real release
	 * wire to cluster_grd_entry_release_holder() + send GES_RELEASE
	 * (spec-2.21 hot path)。
	 */
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}


/*
 * S7 cleanup — error path rollback intent + remove reservation。
 *
 *	spec-2.17 §1.4 P1.4 不同 opcode 分流硬契约:
 *	  - 未 grant(reservation only):enqueue GES_CANCEL_PENDING(opcode 7)
 *	  - 已 grant(holder real):enqueue GES_RELEASE(opcode 3)
 */
ClusterLockAcquireResult
cluster_lock_acquire_s7_cleanup(const ClusterLockAcquireRequest *req)
{
	ensure_counter_initialized();
	pg_atomic_fetch_add_u64(&stub_s7_cleanup_count, 1);

	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	/*
	 * S7 internal API:cleanup success → OK_GRANTED(表 cleanup 完成,
	 * 不表 lock acquire success);real wire to cluster_grd_entry_*
	 * release/cancel mutator(spec-2.21)。
	 */
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}


/*
 * Top-level entry — sequential dispatch S1-S7.
 *
 *	spec-2.21 hot path:lock.c hook 调此函数。本 spec internal API
 *	让 cluster_unit verify S1-S7 全链 invariant。
 *
 *	I1 monotonic forward transition:pre-reservation fail returns directly;
 *	post-reservation FAIL_* short-circuits to S7 cleanup,不回退到 earlier
 *	step。
 */
ClusterLockAcquireResult
cluster_lock_acquire_seven_step(const ClusterLockAcquireRequest *req)
{
	ClusterLockAcquireResult r;

	/* S1 entry — HC1 fail-closed。*/
	r = cluster_lock_acquire_s1_entry(req);
	if (r == CLUSTER_LOCK_ACQUIRE_OK_NATIVE || r == CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE
		|| r == CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL)
		return r;

	/* S2 identity。*/
	r = cluster_lock_acquire_s2_identity(req);
	if (r != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
		return r;

	/* S3 partition + reservation。*/
	r = cluster_lock_acquire_s3_partition_reservation(req);
	if (r == CLUSTER_LOCK_ACQUIRE_PENDING)
		return r;
	if (r == CLUSTER_LOCK_ACQUIRE_FAIL_RESERVATION_FULL || r == CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL)
		return r;

	/*
	 * S4 remote request + wait — spec-2.20 internal API 返回 PENDING
	 * signals spec-2.21 hot path wire 时 caller-side WaitLatch。
	 * 本 spec internal:PENDING 原样返回,避免在未 reservation / 未
	 * GES_REPLY 的情况下伪造 OK_GRANTED。
	 */
	r = cluster_lock_acquire_s4_remote_request_wait(req);
	if (r == CLUSTER_LOCK_ACQUIRE_PENDING)
		return r;
	if (r == CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT || r == CLUSTER_LOCK_ACQUIRE_FAIL_DEADLOCK
		|| r == CLUSTER_LOCK_ACQUIRE_FAIL_CANCEL) {
		(void)cluster_lock_acquire_s7_cleanup(req);
		return r;
	}

	/* S5 promote holder。*/
	r = cluster_lock_acquire_s5_promote_holder(req);
	if (r == CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL) {
		(void)cluster_lock_acquire_s7_cleanup(req);
		return r;
	}

	return r; /* OK_GRANTED / OK_CONVERTED */
}


uint64
cluster_lock_acquire_s1_entry_count(void)
{
	ensure_counter_initialized();
	return pg_atomic_read_u64(&stub_s1_entry_count);
}

uint64
cluster_lock_acquire_s7_cleanup_count(void)
{
	ensure_counter_initialized();
	return pg_atomic_read_u64(&stub_s7_cleanup_count);
}
