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

#include "cluster/cluster_advisory.h" /* spec-5.5 D8 — UL counters */
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_ges.h"
#include "cluster/cluster_ges_reply_wait.h" /* cluster_ges_reply_wait_next_request_id (spec-5.16: node-global request_id) */
#include "cluster/cluster_ges_mode.h" /* spec-5.3 D1 — ges_mode_convert_class for UPGRADE filter */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ir.h" /* spec-5.7 D8 — CLUSTER_IR_RESID_TYPE (freeze-gate bypass belt) */
#include "cluster/cluster_inject.h" /* spec-4.6 L11/L14 — redeclare-skip probe */
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lmd_wait_state.h" /* spec-5.8 D1d — per-proc wait-state */
#include "cluster/cluster_lms.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_native_lock_probe.h"
#include "cluster/cluster_cancel_token.h" /* spec-5.9 D3 cluster_cancel_token_consume */
#include "cluster/cluster_signal.h"		  /* cluster_ges_cancel_pending sig_atomic_t */
#include "storage/latch.h"				  /* spec-4.6 D4 — freeze-gate WaitLatch */
#include "storage/lock.h"				  /* spec-4.6 D3 — LOCALLOCK + GetLockMethodLocalHash */
#include "utils/hsearch.h"				  /* spec-4.6 D3 — hash_seq over LocalLockHash */
#include "utils/wait_event.h"			  /* spec-4.6 D4 — ClusterGrdShardRemaster */
#include "access/htup_details.h"		  /* GETSTRUCT */
#include "access/xact.h"				  /* GetTopTransactionIdIfAny */
#include "catalog/pg_class.h"			  /* Form_pg_class for HC25 relpersistence */
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "utils/syscache.h" /* SearchSysCache1(RELOID, ...) */
#include "utils/timestamp.h"


/*
 * Step counter — diagnostics only(不预设 production-granularity per
 * spec-2.20 v0.4 frozen scope;v0.5 / spec-2.21 真激活时按 Q7 wire
 * grant/reject/convert 3 分项;此 2 counter 是 S1/S7 hot path 计数,
 * 不在 dump_lms 暴露)。
 */
static pg_atomic_uint64 stub_s1_entry_count;
static pg_atomic_uint64 stub_s3_reservation_count;
static pg_atomic_uint64 stub_s4_remote_count;
static pg_atomic_uint64 stub_s5_promote_count;
static pg_atomic_uint64 stub_s6_release_count;
static pg_atomic_uint64 stub_s7_cleanup_count;
static pg_atomic_uint64 stub_local_fast_path_count;
static bool stub_counter_initialized = false;

static inline void
ensure_counter_initialized(void)
{
	if (!stub_counter_initialized) {
		pg_atomic_init_u64(&stub_s1_entry_count, 0);
		pg_atomic_init_u64(&stub_s3_reservation_count, 0);
		pg_atomic_init_u64(&stub_s4_remote_count, 0);
		pg_atomic_init_u64(&stub_s5_promote_count, 0);
		pg_atomic_init_u64(&stub_s6_release_count, 0);
		pg_atomic_init_u64(&stub_s7_cleanup_count, 0);
		pg_atomic_init_u64(&stub_local_fast_path_count, 0);
		stub_counter_initialized = true;
	}
}

/*
 * spec-2.21 D4 — encode ClusterGrdHolderId into LOCALLOCK 24-byte raw image.
 * Mirror: see lock.h LOCALLOCK.cluster_holder_raw definition.
 */
static inline void
fill_request_holder(ClusterLockAcquireRequest *req)
{
	uint64 request_id;

	request_id = req->request_id;
	if (request_id == 0)
		request_id = cluster_ges_reply_wait_next_request_id();

	req->request_id = request_id;
	req->holder.node_id = (uint32)(cluster_node_id >= 0 ? cluster_node_id : 0);
	req->holder.procno = (uint32)(MyProc ? MyProc->pgprocno : 0);
	req->holder.cluster_epoch = cluster_epoch_get_current();
	req->holder.request_id = request_id;
}

/*
 * spec-2.22 D7 — build LMD vertex from caller request.
 *
 *	Identity 4-tuple = (node_id, procno, cluster_epoch, request_id).
 *	Sort metadata = (xid, local_start_ts_ms).  xid may be Invalid for
 *	advisory locks acquired before any write.
 */
static inline void
fill_lmd_vertex_from_request(const ClusterLockAcquireRequest *req, ClusterLmdVertex *out)
{
	memset(out, 0, sizeof(*out));
	out->node_id = (int32)req->holder.node_id;
	out->procno = req->holder.procno;
	out->cluster_epoch = req->holder.cluster_epoch;
	out->request_id = req->request_id;
	out->xid = GetTopTransactionIdIfAny(); /* may be InvalidTransactionId */
	out->local_start_ts_ms = (int64)req->caller_local_start_ts_ms;
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
	ClusterLockAcquireRequest *mut;
	ClusterGrdEntryResult er;
	bool fast_path = false;
	uint64 gen_snapshot = 0;
	int32 self_node = cluster_node_id;

	ensure_counter_initialized();

	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	mut = (ClusterLockAcquireRequest *)req;
	fill_request_holder(mut);

	er = cluster_grd_try_reserve(&req->resid, &req->holder, (int)req->lockmode, self_node,
								 cluster_local_fast_path_enabled ? &fast_path : NULL,
								 &gen_snapshot);
	if (er == CLUSTER_GRD_ENTRY_NOT_READY)
		return CLUSTER_LOCK_ACQUIRE_FAIL_GRD_NOT_READY;
	if (er != CLUSTER_GRD_ENTRY_OK)
		return CLUSTER_LOCK_ACQUIRE_FAIL_RESERVATION_FULL;

	mut->master_gen_snapshot = gen_snapshot;
	pg_atomic_fetch_add_u64(&stub_s3_reservation_count, 1);

	if (cluster_local_fast_path_enabled && fast_path) {
		if (cluster_lms_native_probe_required(&req->resid, req->lockmode)
			&& !cluster_lms_native_probe_wait_clear(&req->resid, req->lockmode, &req->holder,
													/* timeout_ms */ 0)) {
			(void)cluster_grd_cancel_reservation_by_id(&req->resid, &req->holder);
			/* S3 forensics step 1 — attribute the FAIL_TIMEOUT fold. */
			cluster_ges_timeout_detail_set(CLUSTER_GES_TSRC_NATIVE_PROBE_TIMEOUT, self_node, 0, 0,
										   -1, 0);
			return CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
		}
		pg_atomic_fetch_add_u64(&stub_local_fast_path_count, 1);
		return CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	}
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED; /* dispatch to S4 */
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
	uint32 reject;
	bool dontwait;
	bool is_advisory;

	ensure_counter_initialized();

	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	dontwait = req->dontwait;
	is_advisory = (req->locktag.locktag_type == LOCKTAG_ADVISORY);

	pg_atomic_fetch_add_u64(&stub_s4_remote_count, 1);

	/*
	 * spec-5.5 D5 — try-lock (dontwait) sends a conditional GES REQUEST_NOWAIT
	 * (real wire) instead of the spec-2.21 blanket FAIL_TIMEOUT short-circuit.
	 * The master grants if the resource is free, else REJECTs LOCK_CONFLICT
	 * immediately (no waiter enqueued, no BAST).  Blocking acquires take the
	 * enqueue-and-wait path.  Both use the real wire since spec-2.23/2.27.
	 */
	if (dontwait) {
		/* NOWAIT never enqueues a waiter (immediate grant-or-reject) — it
		 * cannot participate in a deadlock, so no wait-state is published. */
		reject = cluster_ges_send_request_nowait_and_wait(&req->resid, (uint32)req->lockmode,
														  &req->holder, req->request_id,
														  req->timeout_ms, req->wait_event);
	} else {
		/*
		 * spec-5.8 D1d — publish the cluster wait-state around the blocking
		 * GES wait so the LMD resolver can confirm this backend is still
		 * waiting before cancelling it as a deadlock victim.  The PG_TRY
		 * clears it on the cancel/ERROR longjmp;  the trailing clear covers
		 * grant / reject / timeout.  request_id + epoch match the master-side
		 * waiter edge (spec-5.8 D1b/D1e).
		 */
		ClusterLmdProcWaitState *ws = (MyProc != NULL) ? &MyProc->cluster_lmd_wait : NULL;

		if (ws != NULL)
			(void)cluster_lmd_wait_state_publish(ws, CLUSTER_LMD_WAIT_GES, req->request_id,
												 cluster_epoch_get_current(),
												 GetTopTransactionIdIfAny());
		PG_TRY();
		{
			reject = cluster_ges_send_request_and_wait(&req->resid, (uint32)req->lockmode,
													   &req->holder, req->request_id,
													   req->timeout_ms, req->wait_event);
		}
		PG_CATCH();
		{
			if (ws != NULL)
				cluster_lmd_wait_state_clear(ws);
			PG_RE_THROW();
		}
		PG_END_TRY();
		if (ws != NULL)
			cluster_lmd_wait_state_clear(ws);
	}
	if (reject == GES_REJECT_REASON_NONE) {
		if (dontwait && is_advisory)
			cluster_advisory_counter_inc(CLUSTER_ADVISORY_TRY_GRANT);
		return CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	}

	/*
	 * spec-5.7 Direction B — dead-master native-safe abort.  The requester's
	 * wait loop proved the target master is CSSD-DEAD and no alive peer remains
	 * (is_sole_native), so the acquire completes on the PG-native path.  Map to
	 * OK_NATIVE (NOT NEED_PG_NATIVE_LOCK): the seven-step dispatcher then runs
	 * S7 to cancel the S3 reservation, and lock.c takes the PG-native lock with
	 * cluster_registered = false.  Using NEED_PG_NATIVE_LOCK would instead run
	 * S5 promote -> register a cluster holder whose 1->0 release would re-send a
	 * GES_RELEASE to the now-dead master (cluster_grd_lookup_master still maps
	 * the resid there — Direction B does not touch mastership) and re-hang.
	 * OK_NATIVE leaves no cluster holder, so the release is native too — the
	 * same end-state as the lock.c gate-time SOLE->native short-circuit.
	 */
	if (reject == GES_REJECT_REASON_MASTER_DEAD_NATIVE)
		return CLUSTER_LOCK_ACQUIRE_OK_NATIVE;

	/*
	 * spec-4.6 D4 — map the REAL GesRejectReason enum.  The previous
	 * mapping used spec-2.21-era placeholder values (1/2/3) that no
	 * longer matched the enum:  a dead-master timeout (TIMEOUT=4)
	 * surfaced as FAIL_INTERNAL "result=16" (pinned by t/249 L4 before
	 * the remaster work landed).
	 */
	switch ((GesRejectReason)reject) {
	case GES_REJECT_REASON_LOCK_CONFLICT:
		/*
		 * spec-5.5 D5 — only the conditional (NOWAIT) path produces a
		 * LOCK_CONFLICT reject; a blocking REQUEST conflict enqueues a waiter
		 * and never REJECTs.  Map the try-lock conflict to NOT_AVAIL (caller
		 * returns false — normal "lock busy", NOT fail-closed §3.5 T3).  A
		 * LOCK_CONFLICT on a blocking request is a protocol violation -> fail
		 * closed (FAIL_INTERNAL).
		 */
		if (dontwait) {
			if (is_advisory)
				cluster_advisory_counter_inc(CLUSTER_ADVISORY_TRY_NOTAVAIL);
			return CLUSTER_LOCK_ACQUIRE_NOT_AVAIL;
		}
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	case GES_REJECT_REASON_WORK_QUEUE_FULL:
		/* Transient master-side capacity — retryable timeout surface. */
		return CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	case GES_REJECT_REASON_EPOCH_MISMATCH:
		/* Stale epoch/generation on the wire (reconfig moved on). */
		cluster_grd_inc_stale_request_drop();
		return CLUSTER_LOCK_ACQUIRE_FAIL_STALE_GENERATION;
	case GES_REJECT_REASON_TIMEOUT:
		return CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	case GES_REJECT_REASON_SHARD_FROZEN:
		/* Master-side recovery gate (the requester-side gate makes this
		 * a narrow race window) — same 53R9I retry surface. */
		return CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING;
	case GES_REJECT_REASON_FEATURE_NOT_SUPPORTED:
		/* spec-5.3 — the cross-node CONVERT path is now live (TM table-lock
		 * upgrade), so FEATURE_NOT_SUPPORTED no longer covers it;  it remains
		 * for genuinely unsupported sub-cases (e.g. cross-node down-convert,
		 * deferred to the CF/PCM block layer).  Not retryable — surfaces as
		 * ERRCODE_FEATURE_NOT_SUPPORTED (0A000). */
		return CLUSTER_LOCK_ACQUIRE_FAIL_FEATURE_NOT_SUPPORTED;
	case GES_REJECT_REASON_DEADLOCK_VICTIM:
		/* spec-5.8 D8 — the LMD coordinator cancelled this backend as a
		 * confirmed cross-node deadlock victim while it blocked in the GES
		 * wait.  Fail closed as a deadlock → lock.c ereports 40P01. */
		return CLUSTER_LOCK_ACQUIRE_FAIL_DEADLOCK;
	case GES_REJECT_REASON_NONE:
	default:
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	}
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
	return cluster_lock_acquire_s5_promote(req);
}

/*
 * spec-5.3 §3.1a — S5 convert (T2):  send opcode-2 CONVERT post-PG-native and
 *	map the GRANT/REJECT.  No reservation to promote (the holder already
 *	exists — it is mutated, not added).  On REJECT/timeout the master never
 *	mutated, so no rollback is needed here;  the caller (lock.c) sends a
 *	best-effort CONVERT_ROLLBACK to close the timeout-race window and keeps the
 *	old weaker hold registered.
 */
static ClusterLockAcquireResult
cluster_lock_acquire_s5_convert(const ClusterLockAcquireRequest *req)
{
	uint32 reject;
	/* spec-5.8 D1d — a cross-node CONVERT (S->X upgrade) blocks and can
	 * deadlock; publish/clear the wait-state around it like the S4 request
	 * wait so the resolver can revalidate the victim. */
	ClusterLmdProcWaitState *ws = (MyProc != NULL) ? &MyProc->cluster_lmd_wait : NULL;

	if (ws != NULL)
		(void)cluster_lmd_wait_state_publish(ws, CLUSTER_LMD_WAIT_GES, req->request_id,
											 cluster_epoch_get_current(),
											 GetTopTransactionIdIfAny());
	PG_TRY();
	{
		reject = cluster_ges_send_convert_and_wait(&req->resid, (uint32)req->lockmode,
												   (uint32)req->current_mode, &req->holder,
												   req->request_id, /* timeout = GUC default */ 0);
	}
	PG_CATCH();
	{
		if (ws != NULL)
			cluster_lmd_wait_state_clear(ws);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (ws != NULL)
		cluster_lmd_wait_state_clear(ws);

	if (reject == GES_REJECT_REASON_NONE) {
		pg_atomic_fetch_add_u64(&stub_s5_promote_count, 1);
		return CLUSTER_LOCK_ACQUIRE_OK_CONVERTED;
	}

	switch ((GesRejectReason)reject) {
	case GES_REJECT_REASON_ILLEGAL_CONVERT:
		return CLUSTER_LOCK_ACQUIRE_FAIL_ILLEGAL_CONVERT;
	case GES_REJECT_REASON_SHARD_FROZEN:
		return CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING;
	case GES_REJECT_REASON_EPOCH_MISMATCH:
		return CLUSTER_LOCK_ACQUIRE_FAIL_STALE_GENERATION;
	case GES_REJECT_REASON_DEADLOCK_VICTIM:
		/* spec-5.8 D8 — converter cancelled as a cross-node deadlock victim. */
		return CLUSTER_LOCK_ACQUIRE_FAIL_DEADLOCK;
	case GES_REJECT_REASON_TIMEOUT:
	case GES_REJECT_REASON_WORK_QUEUE_FULL:
	default:
		return CLUSTER_LOCK_ACQUIRE_FAIL_TIMEOUT;
	}
}

/*
 * spec-2.21 D4 P2.3 — S5 promote with revalidate;失败 5-step backout.
 */
ClusterLockAcquireResult
cluster_lock_acquire_s5_promote(const ClusterLockAcquireRequest *req)
{
	ClusterGrdEntryResult er;
	int32 self_node = cluster_node_id;

	ensure_counter_initialized();

	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	/* spec-5.3 §3.1a — convert path mutates an existing holder (no
	 * reservation to promote);  send the CONVERT wire at S5 (T2). */
	if (req->op == CLUSTER_LOCK_OP_CONVERT)
		return cluster_lock_acquire_s5_convert(req);

	er = cluster_grd_revalidate_and_promote(&req->resid, &req->holder, self_node,
											req->master_gen_snapshot);
	if (er != CLUSTER_GRD_ENTRY_OK) {
		(void)cluster_lock_acquire_s7_cleanup(req);
		/*
		 * PGRAC: spec-5.3 D1 — a LOCKTAG_TRANSACTION ShareLock waiter
		 * (XactLockTableWait) that finds the GRD entry gone at S5 promote means
		 * the awaited transaction already finished (its PG-native ShareLock was
		 * granted only after the holder released).  The wait is satisfied, so
		 * this is a benign success, NOT an internal error.  Strictly narrowed by
		 * the helper to {LOCKTAG_TRANSACTION, ShareLock, NOT_FOUND}.
		 */
		if (cluster_lock_acquire_s5_not_found_is_benign((uint8)req->locktag.locktag_type,
														req->lockmode, er))
			return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;
	}

	pg_atomic_fetch_add_u64(&stub_s5_promote_count, 1);
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}


/*
 * S6 release — backend done(LockRelease hook;spec-2.21 wire to PG)。
 */
ClusterLockAcquireResult
cluster_lock_acquire_s6_release(const ClusterLockAcquireRequest *req)
{
	int32 master;

	ensure_counter_initialized();

	if (req == NULL)
		return CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL;

	/*
	 * PGRAC: spec-5.5 P0 — route the release the SAME way the acquire/send path
	 * routed the request (ges_send_request_opcode_and_wait treats
	 * master < 0 || master == cluster_node_id as local-master).  Keeping the two
	 * conditions identical is the invariant:  whatever acquire enqueued as a
	 * LOCAL waiter (including the master-map-not-yet-initialized master < 0
	 * fallback) must be drained+woken LOCALLY on release.  A bare == self check
	 * would, under master < 0, fall to the remote branch and only delete the
	 * holder — re-stranding the waiter (the exact bug this spec fixes).
	 */
	master = cluster_grd_lookup_master(&req->resid);
	if (master < 0 || master == cluster_node_id) {
		/*
		 * Local master.  The GRD entry (holder + any queued blocking waiters)
		 * lives on THIS node, so the release must drain + grant + WAKE the
		 * waiters, exactly like the remote GES_RELEASE handler does for a remote
		 * master.  release_and_drain itself removes the holder, so we do NOT call
		 * cluster_grd_release_holder_by_id here (that deletes the holder without
		 * popping a waiter -> a cross-node blocking pg_advisory_lock() waiter
		 * mastered here would false-timeout 53R70 / hang on -1).
		 */
		cluster_ges_release_and_drain_local(&req->resid, &req->holder);
	} else {
		/*
		 * Remote master: the holder + waiters live on the master.  Drop the
		 * (no-op for remote-mastered resources) local holder view, then send
		 * GES_RELEASE (bounded ACK wait);  the master's drain handler runs the
		 * authoritative drain + wake.
		 */
		(void)cluster_grd_release_holder_by_id(&req->resid, &req->holder);
		(void)cluster_ges_send_release_and_wait(&req->resid, &req->holder, req->request_id);
	}

	pg_atomic_fetch_add_u64(&stub_s6_release_count, 1);
	return CLUSTER_LOCK_ACQUIRE_OK_GRANTED;
}

/*
 * spec-2.21 D1/D6 — normal release entry point.
 *
 *	D2/D3 hooks (LockRelease / LockReleaseAll / ResourceOwnerRelease) call
 *	this with the LOCALLOCK->cluster_* state when nLocks == 1.  Walks S6 +
 *	wait-edge cancel stub.  Exactly-once gated by caller via
 *	LOCALLOCK->cluster_registered (HC9).
 */
void
cluster_lock_release(const ClusterLockReleaseRequest *req)
{
	ClusterLockAcquireRequest internal;

	if (req == NULL || !req->cluster_registered)
		return;

	/* spec-5.5 D8 — UL session-release counter:  a session-scoped advisory
	 * holder is being drained (per-key unlock / unlock_all via the :2214 edge
	 * hook, or backend-exit via the LockReleaseAll drain). */
	if (req->sessionLock && req->locktag.locktag_type == LOCKTAG_ADVISORY)
		cluster_advisory_counter_inc(CLUSTER_ADVISORY_SESSION_RELEASE);

	memset(&internal, 0, sizeof(internal));
	internal.locktag = req->locktag;
	internal.lockmode = req->lockmode;
	internal.sessionLock = req->sessionLock;
	internal.holder = req->holder;
	internal.request_id = req->request_id;
	cluster_grd_resid_encode(&req->locktag, &internal.resid);

	(void)cluster_lock_acquire_s6_release(&internal);

	/* spec-2.22 D7:remove wait edge by waiter identity (real wire). */
	{
		ClusterLmdVertex v;
		fill_lmd_vertex_from_request(&internal, &v);
		cluster_lmd_cancel_wait_edge_real(&v);
	}
	cluster_lmd_cancel_wait_edge(); /* keep stub counter ++ for spec-2.19 compat */
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
	 * spec-2.21 D4 — S7 cleanup:cancel any outstanding reservation +
	 * wait-edge stub.  No-op safe if no reservation present (idempotent).
	 *
	 * spec-2.22 D7 — also remove real wait edge from LMD graph by waiter
	 * identity 4-tuple.
	 */
	(void)cluster_grd_cancel_reservation_by_id(&req->resid, &req->holder);
	{
		ClusterLmdVertex v;
		fill_lmd_vertex_from_request(req, &v);
		cluster_lmd_cancel_wait_edge_real(&v);
	}
	cluster_lmd_cancel_wait_edge();
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

	/* S3 forensics step 1 — clear the backend-local timeout-source detail at
	 * the single dispatch funnel so a FAIL_TIMEOUT surfaced by lock.c never
	 * reads a stale attribution from a previous acquire. */
	cluster_ges_timeout_detail_reset();

	/*
	 * spec-2.22 D8 / spec-5.9 D3 — cancel check point (a): seven-step top
	 * dispatch entry.  cluster_cancel_token_consume() honors the cancel ONLY
	 * when the installed token matches this backend's live wait-state; at the
	 * loop top the backend is not yet waiting, so a stale / late signal (e.g.
	 * one that raced a prior grant + slot reuse) is cleared without raising
	 * 40P01 (Rule 8.A P0#1 — never kill the wrong transaction).  Honor => run S7
	 * cleanup, return FAIL_DEADLOCK so lock.c ereports 40P01 after cleanup.
	 *
	 *	HC14 + L118 — never ereport in the ProcessInterrupts handler; cleanup
	 *	→ S7 → outer caller ereport.
	 */
	if (cluster_cancel_token_consume()) {
		(void)cluster_lock_acquire_s7_cleanup(req);
		return CLUSTER_LOCK_ACQUIRE_FAIL_DEADLOCK;
	}

	/* S1 entry — HC1 fail-closed。*/
	r = cluster_lock_acquire_s1_entry(req);
	if (r == CLUSTER_LOCK_ACQUIRE_OK_NATIVE || r == CLUSTER_LOCK_ACQUIRE_FAIL_LMS_UNAVAILABLE
		|| r == CLUSTER_LOCK_ACQUIRE_FAIL_INTERNAL)
		return r;

	/* S2 identity。*/
	r = cluster_lock_acquire_s2_identity(req);
	if (r != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
		return r;

	/*
	 * spec-4.6 D4 — shard recovery freeze gate (requester side).
	 *
	 *	A shard in FROZEN/REBUILDING phase must not accept new grants:
	 *	the holder rebuild (D3) is still filling holders[], and a grant
	 *	decided against the partial set could double grant.  Short-wait
	 *	up to cluster.grd_remaster_wait_ms (wait event
	 *	ClusterGrdShardRemaster) — most remasters complete sub-second —
	 *	then fail closed (53R9I at the lock.c caller; application
	 *	retries).  dontwait (ConditionalLock) requests fail immediately.
	 *	The CFI inside the wait loop also lets this backend run its own
	 *	cooperative rebind if it is part of the same recovery episode.
	 */
	{
		uint32 gate_shard = cluster_grd_shard_for_resource(&req->resid);

		/*
		 * spec-5.7 D8 (IR-M5) fresh-epoch bypass: an IR instance-recovery acquire
		 * runs inside the freeze window (the recovery worker takes IR(X) while its
		 * shard is still REBUILDING, and P7->NORMAL is itself gated on that recovery
		 * finishing).  A (dead_node, NEW_epoch) IR resid is brand new this episode,
		 * so there is NO holder set being rebuilt for it -- the freeze gate's only
		 * purpose (not granting against a half-rebuilt holder set) is vacuous.  Skip
		 * the phase check for it (flag + resid-type belt); the normal grant/conflict
		 * path below is unchanged, so cross-survivor mutual exclusion still holds.
		 */
		bool ir_bootstrap_bypass
			= (req->recovery_bootstrap && req->resid.type == CLUSTER_IR_RESID_TYPE);

		if (!ir_bootstrap_bypass && cluster_grd_shard_phase(gate_shard) != GRD_SHARD_NORMAL) {
			TimestampTz gate_deadline;

			if (req->dontwait)
				return CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING;

			gate_deadline
				= TimestampTzPlusMilliseconds(GetCurrentTimestamp(), cluster_grd_remaster_wait_ms);
			for (;;) {
				if (cluster_grd_shard_phase(gate_shard) == GRD_SHARD_NORMAL)
					break;
				if (GetCurrentTimestamp() >= gate_deadline)
					return CLUSTER_LOCK_ACQUIRE_FAIL_SHARD_REMASTERING;
				(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 10,
								WAIT_EVENT_CLUSTER_GRD_SHARD_REMASTER);
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
			}
		}
	}

	/*
	 * spec-5.3 §3.1a — CONVERT (same-backend upgrade) defers to PG-native
	 * first (T1):  the holder already exists, so there is NO S3 reservation
	 * (a reservation would add a second holder, not mutate the existing one).
	 * Mint the convert reply key (R_new) + holder identity here, then return
	 * NEED_PG_NATIVE_LOCK so lock.c runs the local PG-native LockAcquire and
	 * then S5 (which sends the opcode-2 CONVERT — T2).
	 */
	if (req->op == CLUSTER_LOCK_OP_CONVERT) {
		ClusterLockAcquireRequest *mut = (ClusterLockAcquireRequest *)req;

		fill_request_holder(mut);
		return CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK;
	}

	/*
	 * S3 partition + reservation.  Returns:
	 *   - NEED_PG_NATIVE_LOCK with local-fast-path: short-circuit success,
	 *     caller does PG-native + S5 promote.
	 *   - OK_GRANTED:  remote-master path, fall through to S4.
	 *   - FAIL_*:  pre-reservation fail, do NOT run S7 cleanup
	 *              (no reservation to cancel — F2 invariant from spec-2.20).
	 */
	r = cluster_lock_acquire_s3_partition_reservation(req);
	if (r == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK)
		return r; /* lock.c will call S5 post-PG-native. */
	if (r != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
		return r; /* pre-reservation fail */

	/* S4 remote-master path (reservation already created). */
	r = cluster_lock_acquire_s4_remote_request_wait(req);
	if (r == CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK)
		return r;

	/*
	 * spec-4.6 P0 (user review) — DEFAULT-DENY after S4.
	 *
	 *	Only the explicit success outcomes may continue toward S5
	 *	promote.  The previous FAIL_* allowlist (TIMEOUT / DEADLOCK /
	 *	CANCEL / INTERNAL) silently let any value outside it fall
	 *	through:  when spec-4.6 added FAIL_SHARD_REMASTERING /
	 *	FAIL_STALE_GENERATION, a master that explicitly REJECTED the
	 *	request (frozen shard / stale generation) still saw the caller
	 *	promote its local reservation into a holder — fail-OPEN on the
	 *	exact paths the remaster protocol must fail closed.  Any
	 *	non-success S4 outcome now cancels the reservation (S7) and
	 *	surfaces the original error;  a future new FAIL code cannot
	 *	re-open this hole.
	 */
	if (r != CLUSTER_LOCK_ACQUIRE_OK_GRANTED && r != CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
		(void)cluster_lock_acquire_s7_cleanup(req);
		return r;
	}

	/*
	 * Legacy path:  if S4 returned OK_GRANTED (older stubs), still need
	 * S5 promote to convert reservation -> holder.
	 */
	r = cluster_lock_acquire_s5_promote_holder(req);
	if (r != CLUSTER_LOCK_ACQUIRE_OK_GRANTED && r != CLUSTER_LOCK_ACQUIRE_OK_CONVERTED) {
		(void)cluster_lock_acquire_s7_cleanup(req);
	}
	return r;
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

/*
 * spec-2.21 D4 P2.4 — dump 6 emit_row(s1_entry / s3_reservation / s4_remote /
 *	s5_promote / s6_release / s7_cleanup).  030_acceptance L18 HC9 grant-
 *	release 对称 acceptance gate consumes these.
 */
void
cluster_lock_acquire_dump(void (*emit_row)(void *cookie, const char *key, const char *value),
						  void *cookie)
{
	char buf[32];

	ensure_counter_initialized();

	snprintf(buf, sizeof(buf), UINT64_FORMAT, pg_atomic_read_u64(&stub_s1_entry_count));
	emit_row(cookie, "s1_entry", buf);
	snprintf(buf, sizeof(buf), UINT64_FORMAT, pg_atomic_read_u64(&stub_s3_reservation_count));
	emit_row(cookie, "s3_reservation", buf);
	snprintf(buf, sizeof(buf), UINT64_FORMAT, pg_atomic_read_u64(&stub_s4_remote_count));
	emit_row(cookie, "s4_remote", buf);
	snprintf(buf, sizeof(buf), UINT64_FORMAT, pg_atomic_read_u64(&stub_s5_promote_count));
	emit_row(cookie, "s5_promote", buf);
	snprintf(buf, sizeof(buf), UINT64_FORMAT, pg_atomic_read_u64(&stub_s6_release_count));
	emit_row(cookie, "s6_release", buf);
	snprintf(buf, sizeof(buf), UINT64_FORMAT, pg_atomic_read_u64(&stub_s7_cleanup_count));
	emit_row(cookie, "s7_cleanup", buf);
}


/*
 * spec-2.25 D1 — relpersistence helper for cluster_lock_should_globalize HC25.
 *
 *	Returns true if the relation is unlogged or permanent (eligible for
 *	cluster gate);  false if temp (must stay PG-native).  Cache-miss
 *	defaults to permanent (HC25 fail-safe — over-acquire is correct;
 *	under-acquire is a correctness bug because LMS visibility hole).
 *
 *	Cost:  SearchSysCache1(RELOID, oid) is ~50ns on hot catcache;  fires
 *	only on lockmode >= ShareUpdateExclusiveLock + oid >= FirstNormalObjectId
 *	paths (Q2 + Q3 + Q4 enforce — OLTP SELECT / INSERT / UPDATE / DELETE
 *	short-circuits before reaching this helper).
 *
 *	Called from cluster_lock_acquire.h inline gate;  not a hot OLTP path.
 */
bool
cluster_relation_is_persistent_or_unlogged(Oid relid)
{
	HeapTuple tuple;
	bool eligible;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple)) {
		/* HC25 fail-safe:  unknown oid → assume permanent (cluster gate
		 * over-acquires defensively rather than skipping). */
		return true;
	}

	{
		Form_pg_class rel = (Form_pg_class)GETSTRUCT(tuple);
		char persistence = rel->relpersistence;

		/* RELPERSISTENCE_PERMANENT 'p' + RELPERSISTENCE_UNLOGGED 'u' route
		 * through cluster;  RELPERSISTENCE_TEMP 't' skips. */
		eligible = (persistence != RELPERSISTENCE_TEMP);
	}

	ReleaseSysCache(tuple);
	return eligible;
}


/*
 * spec-6.14 D7 — is relid a mapped relation?
 *
 *	A mapped relation carries relfilenode == InvalidOid in its pg_class row;
 *	its real file number lives in pg_filenode.map (the nailed system catalogs
 *	that VACUUM FULL can rewrite).  cluster_lock_should_globalize uses this to
 *	globalize mapped-relation write-mode locks (>= RowExclusive) under
 *	shared_catalog, so a peer's low-mode write conflicts cross-node with a
 *	relmap rewrite's AEL (r3 self-disclosure).
 *
 *	Fires only on catalog OIDs at RowExclusive+ (DDL-frequency; never the OLTP
 *	user-table path).  Fail-safe:  unknown oid -> true (over-globalize is safe;
 *	under-globalize risks a lost write on a mapped rel).
 */
bool
cluster_relation_is_mapped(Oid relid)
{
	HeapTuple tuple;
	bool mapped;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return true; /* fail-safe: assume mapped (over-globalize is safe) */

	{
		Form_pg_class rel = (Form_pg_class)GETSTRUCT(tuple);

		mapped = (rel->relfilenode == InvalidOid);
	}

	ReleaseSysCache(tuple);
	return mapped;
}


/*
 * cluster_lock_decide_op — spec-5.3 D1: REQUEST vs CONVERT (requester-local).
 *
 *	Scans this backend's own LockMethodLocalHash for a cluster_registered
 *	LOCALLOCK on the SAME locktag whose mode is strictly weaker than the
 *	requested mode.  Detection is requester-local (NOT a GRD read): the GRD
 *	entry lives at the resource's master, which may be remote, and the
 *	requester only sends to it — it cannot inspect a remote holders[].  The
 *	backend's own held-lock state is authoritative for "do I already hold a
 *	weaker mode on this resource".  Returns CONVERT iff such a hold exists and
 *	cluster.tm_convert_mode = 'convert';  *current_mode_out = the STRONGEST
 *	already-held weaker cluster-registered mode (the REDECLARE locator key) and
 *	*weaker_locallock_out = that LOCALLOCK (so the caller can de-register it on
 *	a successful convert — §3.1a).
 */
ClusterLockOp
cluster_lock_decide_op(const ClusterLockAcquireRequest *req, LOCKMODE *current_mode_out,
					   LOCALLOCK **weaker_locallock_out)
{
	HTAB *locallocks;
	HASH_SEQ_STATUS status;
	LOCALLOCK *locallock;
	LOCALLOCK *best = NULL;
	LOCKMODE best_mode = NoLock;

	if (current_mode_out != NULL)
		*current_mode_out = NoLock;
	if (weaker_locallock_out != NULL)
		*weaker_locallock_out = NULL;

	if (req == NULL)
		return CLUSTER_LOCK_OP_REQUEST;
	/*
	 * spec-5.5 §3.8 MO2 / Q10 — advisory (user) locks NEVER convert.  PG treats
	 * advisory ShareLock and ExclusiveLock on the same key as independent
	 * additive holds (a session may hold both), so acquiring a stronger advisory
	 * mode while holding a weaker one on the same key is a second independent
	 * holder (REQUEST), not an upgrade.  Routing it through the blocking GES
	 * convert (S5 cluster_ges_send_convert_and_wait) would also break
	 * pg_try_advisory_lock's non-blocking contract — a try-lock must return
	 * true/false immediately, never enqueue a waiter and wait.
	 */
	if (req->locktag.locktag_type == LOCKTAG_ADVISORY)
		return CLUSTER_LOCK_OP_REQUEST;
	/* spec-5.3 Q1=B escape hatch — additive keeps the current REQUEST model. */
	if (cluster_tm_convert_mode != CLUSTER_TM_CONVERT_MODE_CONVERT)
		return CLUSTER_LOCK_OP_REQUEST;
	/* Fast out (review P3-2): no cluster-registered locks held -> no weaker
	 * hold to convert, so skip the LocalLockHash scan on the acquire hot path. */
	if (MyProc == NULL || pg_atomic_read_u32(&MyProc->cluster_grd_registered_count) == 0)
		return CLUSTER_LOCK_OP_REQUEST;

	locallocks = GetLockMethodLocalHash();
	if (locallocks == NULL)
		return CLUSTER_LOCK_OP_REQUEST;

	hash_seq_init(&status, locallocks);
	while ((locallock = (LOCALLOCK *)hash_seq_search(&status)) != NULL) {
		if (!locallock->cluster_registered || locallock->nLocks == 0)
			continue;
		if (memcmp(&locallock->tag.lock, &req->locktag, sizeof(LOCKTAG)) != 0)
			continue; /* different resource */
		/*
		 * Only a genuine partial-order UPGRADE is a convert (review): route by
		 * the frozen-matrix convert class, NOT a numeric mode comparison.  A
		 * held mode that is merely numerically weaker but LATERAL (incomparable)
		 * to the requested mode — e.g. SHARE UPDATE EXCLUSIVE held, SHARE
		 * requested — is a legitimate PG additive multi-mode hold, so it must
		 * fall through to an additive REQUEST (a second holder), NOT be forced
		 * into a convert that the master would reject 53R74.  Pick the strongest
		 * UPGRADE-eligible held mode as the locator.
		 */
		if (ges_mode_convert_class((ClusterGesMode)locallock->tag.mode,
								   (ClusterGesMode)req->lockmode)
			!= GES_CONVERT_UPGRADE)
			continue;
		if (best == NULL || locallock->tag.mode > best_mode) {
			best = locallock;
			best_mode = locallock->tag.mode;
		}
	}

	if (best == NULL)
		return CLUSTER_LOCK_OP_REQUEST;

	if (current_mode_out != NULL)
		*current_mode_out = best_mode;
	if (weaker_locallock_out != NULL)
		*weaker_locallock_out = best;
	return CLUSTER_LOCK_OP_CONVERT;
}


/*
 * cluster_grd_redeclare_all_registered — spec-4.6 D3 backend walker.
 *
 *	Runs at CFI (cluster_grd_check_pending_interrupts) after LMON
 *	broadcast PROCSIG_CLUSTER_GRD_REDECLARE.  Walks THIS backend's
 *	LocalLockHash (the only process that legally can — lock.h:454
 *	backend-private), and for every cluster_registered LOCALLOCK
 *	performs the old→new holder rebind (§2.3):
 *
 *	  1. the stored cluster_holder_raw (old epoch) is the PROOF this
 *	     backend held the grant — it selects WHAT to redeclare;  its
 *	     stale epoch/request_id never ride the wire;
 *	  2. a NEW holder is minted from the CURRENT accepted epoch + a
 *	     fresh request_id (routing generation is recomputed by the send
 *	     path — never exported from the stale LOCALLOCK);
 *	  3. the new holder goes to the resource's CURRENT master
 *	     (post-remaster map):  local master → direct insert-or-rebind,
 *	     remote master → GES_REDECLARE send-and-wait;
 *	  4. ONLY on ack are cluster_holder_raw / cluster_request_id
 *	     overwritten, so the later release path (lock.c) presents the
 *	     current-epoch holder.  No-ack → old holder kept, no barrier
 *	     ack → the shard stays frozen (fail-closed).
 *
 *	Scope is CLUSTER-WIDE (P0#3):  every registered grant rebinds, not
 *	only those on remastered shards — the epoch bump staled every
 *	stored identity, and an un-rebound holder on an UNAFFECTED shard
 *	would fail its own release/convert forever.
 *
 *	No-throw contract:  runs from ProcessInterrupts (including the
 *	DoingCommandRead idle path);  all failures are swallowed into
 *	"don't ack" — LMON re-broadcasts until the barrier completes.
 */
void
cluster_grd_redeclare_all_registered(void)
{
	uint64 gen;
	uint64 cur_epoch;
	HTAB *locallocks;
	HASH_SEQ_STATUS status;
	LOCALLOCK *locallock;
	bool all_ok = true;

	if (!cluster_enabled || MyProc == NULL)
		return;

	/* spec-4.6 L11/L14 — armed backend neither rebinds nor acks:  the
	 * barrier stalls fail-closed (rebuild_timeout + frozen shards), and
	 * a terminated armed backend leaves the P6 leak-sweep evidence.
	 * STICKY probe (cluster_cr_injection_armed peeks without consuming;
	 * the generic should_skip is one-shot and could not hold the barrier
	 * across LMON's re-broadcast ticks). */
	{
		uint64 skip_param;

		if (cluster_cr_injection_armed("cluster-grd-redeclare-skip", &skip_param))
			return;
	}

	gen = cluster_grd_redeclare_generation();
	if (gen == 0)
		return; /* no barrier ever armed */
	if (pg_atomic_read_u64(&MyProc->cluster_grd_redeclare_acked) >= gen)
		return; /* already acked this generation */

	cur_epoch = cluster_epoch_get_current();
	locallocks = GetLockMethodLocalHash();
	if (locallocks != NULL) {
		hash_seq_init(&status, locallocks);
		while ((locallock = (LOCALLOCK *)hash_seq_search(&status)) != NULL) {
			ClusterGrdHolderId old_holder;
			ClusterGrdHolderId new_holder;
			ClusterResId resid;
			int32 master;
			uint64 fresh_request_id;
			bool ok = false;

			if (!locallock->cluster_registered)
				continue;

			/*
			 * Release-in-flight guard:  the lock.c cluster release hook
			 * runs at the nLocks 1→0 edge, and its GES_RELEASE wait can
			 * CFI into this walker.  Rebinding a lock whose release is
			 * already in flight would mint a CURRENT-epoch holder the
			 * (old-epoch) release can never remove — a leak even P6
			 * cannot sweep.  Skip it:  the old-epoch master-side slot
			 * stays leaked-by-old-epoch and P6 sweeps it as designed.
			 */
			if (locallock->nLocks == 0)
				continue;

			memcpy(&old_holder, locallock->cluster_holder_raw, sizeof(old_holder));
			if (old_holder.cluster_epoch >= cur_epoch)
				continue; /* already current (idempotent re-entry) */

			cluster_grd_resid_encode(&locallock->tag.lock, &resid);

			fresh_request_id = cluster_ges_reply_wait_next_request_id();
			new_holder.node_id = old_holder.node_id;
			new_holder.procno = old_holder.procno;
			new_holder.cluster_epoch = cur_epoch;
			new_holder.request_id = fresh_request_id;

			master = cluster_grd_lookup_master(&resid);
			if (master == cluster_node_id) {
				/* Local master (incl. shard just remastered TO this
				 * node):  direct insert-or-rebind, no wire. */
				ok = (cluster_grd_entry_rebind_or_insert_holder(
						  &resid, &new_holder, cluster_node_id, (int)locallock->tag.mode)
					  == CLUSTER_GRD_ENTRY_OK);
			} else if (master >= 0) {
				ok = (cluster_ges_send_redeclare_and_wait(&resid, (uint32)locallock->tag.mode,
														  &new_holder, fresh_request_id)
					  == 0);
			}

			if (ok) {
				/* §2.3 step 4 — overwrite ONLY after the master acked. */
				memcpy(locallock->cluster_holder_raw, &new_holder,
					   sizeof(locallock->cluster_holder_raw));
				locallock->cluster_request_id = fresh_request_id;
			} else {
				all_ok = false;
			}
		}
	}

	/*
	 * P0-1 epoch coherence:  ack ONLY if the epoch did not move while we
	 * walked.  If it did, the holders we just stamped are already stale;
	 * leaving this proc un-acked makes LMON's barrier wait, and the next
	 * generation (re-broadcast under the new episode epoch) re-walks us.
	 * We publish the ack EPOCH alongside the generation so the barrier
	 * can reject an ack that pre-dates a mid-episode epoch bump.
	 */
	if (all_ok && cluster_epoch_get_current() == cur_epoch) {
		pg_atomic_write_u64(&MyProc->cluster_grd_redeclare_acked_epoch, cur_epoch);
		pg_atomic_write_u64(&MyProc->cluster_grd_redeclare_acked, gen);
	}
}
