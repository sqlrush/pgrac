/*-------------------------------------------------------------------------
 *
 * cluster_native_lock_probe.h
 *	  pgrac per-node native-lock probe — spec-2.25 D2.
 *
 *	  Background:  spec-2.25 extends cluster_lock_should_globalize to
 *	  cover LOCKTAG_RELATION + LOCKTAG_OBJECT (lockmode >= SUEX).  Modes
 *	  <= RowExclusiveLock continue to use PG-native LockAcquireExtended
 *	  only (no cluster registration).  This leaves an LMS visibility hole:
 *	  before granting a cluster-mode acquire, LMS must verify that no
 *	  remote PG-native low-mode holder (or waiter) on the same LOCKTAG
 *	  blocks the cluster grant.  Native-lock probe (HC30..HC32a) closes
 *	  this hole by asking each live node to scan its local shared PG lock
 *	  state and report CLEAR / HOLDER_CONFLICT / WAITER_CONFLICT.
 *
 *	  Probe target structures (HC30, lock.c-internal scan via D8):
 *	    1. LockMethodLockHash holder PROCLOCKs (shared partitioned hash)
 *	    2. LOCK->waitProcs wait queue (per-LOCK proc queue)
 *	    3. relation fast-path holders (FastPathStrongRelationLocks +
 *	       per-PGPROC fpRelId bitmap;  only scanned when
 *	       ConflictsWithRelationFastPath returns true)
 *	    NOTE:  LockMethodLocalHash is backend-private and MUST NOT be
 *	    used as a native probe source — cluster aux processes have no
 *	    LOCALLOCK state of their own, so scanning their LocalHash returns
 *	    nothing useful (silent CLEAR → false grant).  HC30 子条 4.
 *
 *	  Origin self-probe (HC32a, Q12=B):  the LMS process is not the
 *	  requester backend, so it cannot rely on PG GetLockConflicts()'s
 *	  implicit MyProc exclusion.  Callers must supply
 *	  ClusterGrdHolderId *exclude_holder identifying the originating
 *	  backend (node_id + procno + cluster_epoch + request_id) so the
 *	  probe correctly skips locks held by the requester itself (e.g.,
 *	  same-xact lock-mode escalation:  RowExcl held by INSERT, then
 *	  AccessExcl requested by ALTER TABLE — without exclusion the
 *	  requester would self-conflict and 53R83 timeout).
 *
 *	  Wire protocol:  cluster_ges.h GES_REQ_OPCODE_NATIVE_LOCK_PROBE (9)
 *	  + NATIVE_LOCK_PROBE_REPLY (10) with 32B fixed payloads.  Dispatch
 *	  via cluster_ges_request_handler early opcode fork (mirrors
 *	  DEADLOCK_PROBE / DEADLOCK_REPORT).  Bespoke HC33 dual-source +
 *	  epoch + quorum validation (does not use the generic 1..7 main
 *	  ges_validate_inbound path).
 *
 *	  LMS-side collector slot lives in cluster_lms.c (D4 — per-shard
 *	  bounded array sized by cluster.lms_native_lock_probe_max_inflight
 *	  GUC).  Retry-poll loop on
 *	  cluster.lms_native_lock_probe_retry_interval_ms with
 *	  cluster.lms_native_lock_probe_retry_budget cap;  budget exceeded
 *	  → SQLSTATE 53R83 ERRCODE_CLUSTER_NATIVE_LOCK_PROBE_TIMEOUT.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * IDENTIFICATION
 *	  src/include/cluster/cluster_native_lock_probe.h
 *
 * NOTES
 *	  spec-2.25 D2 — interface header (Step 3 骨架 ship + Step 4 D3
 *	  body 真激活).  Step 1-2 已 wire opcode dispatch + outbound origin;
 *	  Step 3 ship API prototype + collector slot struct;  Step 4 添加
 *	  cluster_native_lock_probe_local body via D8 lock.c helper;  Step 5
 *	  ship LMS collector lifecycle.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_NATIVE_LOCK_PROBE_H
#define CLUSTER_NATIVE_LOCK_PROBE_H

#ifndef FRONTEND

#include "postgres.h"
#include "storage/lock.h"
#include "cluster/cluster_grd.h" /* ClusterGrdHolderId */

/* ============================================================
 * spec-2.25 D2 §2.2 — probe reply 3-state enum (HC31).
 *
 *	CLEAR < WAITER_CONFLICT < HOLDER_CONFLICT priority — LMS aggregates
 *	per-node responses via max() (any HOLDER or WAITER → defer grant).
 *
 *	Wire ABI value lock — int32 reply.status carries this directly.
 * ============================================================ */
typedef enum ClusterNativeLockProbeReply {
	CLUSTER_NATIVE_LOCK_PROBE_CLEAR = 0,
	CLUSTER_NATIVE_LOCK_PROBE_WAITER_CONFLICT = 1,
	CLUSTER_NATIVE_LOCK_PROBE_HOLDER_CONFLICT = 2
} ClusterNativeLockProbeReply;

/* ============================================================
 * spec-2.25 D3 — local probe entry point.
 *
 *	Scans node-local shared PG lock state(spec-2.25 D3/D8 HC30..HC32):
 *	  1. LockMethodLockHash holder PROCLOCKs
 *	  2. LOCK->waitProcs waiter queue
 *	  3. relation fast-path PGPROC fpRelId bitmap (only when
 *	     ConflictsWithRelationFastPath returns true)
 *
 *	LockMethodLocalHash is backend-private and MUST NOT be used for
 *	cross-backend probe correctness (HC30 子条 4).
 *
 *	exclude_holder identifies the originating backend/request and prevents
 *	the requester from self-conflicting (HC32a).  Caller must pass a
 *	stable 4-tuple {node_id, procno, cluster_epoch, request_id};  fail
 *	closed if exclude_holder is NULL (Step 4 build asserts).
 * ============================================================ */
extern ClusterNativeLockProbeReply
cluster_native_lock_probe_local(const LOCKTAG *locktag, LOCKMODE lockmode,
								const ClusterGrdHolderId *exclude_holder);

/* ============================================================
 * spec-2.25 D8 — lock.c-internal scanner (HC30 partition-lock-safe).
 *
 *	Lives in src/backend/storage/lmgr/lock.c so it can access static
 *	LockMethodLockHash / FastPathStrongRelationLocks / etc. PG types.
 *	Called by cluster_native_lock_probe_local (D3) wrapper.
 *
 *	exclude_holder MUST be non-NULL (HC32a);  NULL → fail-closed
 *	HOLDER_CONFLICT defensive return.
 * ============================================================ */
extern ClusterNativeLockProbeReply
cluster_native_lock_probe_pg_state(const LOCKTAG *locktag, LOCKMODE lockmode,
								   const ClusterGrdHolderId *exclude_holder);

extern bool cluster_lms_native_probe_required(const ClusterResId *resid, LOCKMODE lockmode);

/* ============================================================
 * spec-2.25 D4 — LMS per-shard collector slot.
 *
 *	One slot tracks a single in-flight probe:  expected reply set
 *	bitmask over N-1 peer nodes + aggregated status + retry/timeout
 *	bookkeeping.  Per-shard array sized by GUC
 *	cluster.lms_native_lock_probe_max_inflight (default 8).
 *
 *	Lifecycle:
 *	  acquire_slot → dispatch → recv_reply (N-1 times) →
 *	  aggregate_and_resolve → release_slot
 *
 *	HC36 stale-reply drop:  probe_id is the slot's monotonic id;
 *	reply with probe_id ∉ active slots → silent drop + counter ++.
 *
 *	HC35 fence-gated dead-node cleanup:  cleanup_on_node_dead clears
 *	the dead bit and re-aggregates, but ONLY after CSSD/fence + GRD
 *	cleanup_on_node_dead generation completes.  Premature CLEAR-on-dead
 *	risks split-brain false grant.
 * ============================================================ */

/* StaticAssertDecl on the slot struct lives next to the definition in
 * cluster_lms.h to keep the size invariant near its implementation. */

extern bool cluster_lms_native_probe_slot_acquire(int32 origin_node_id, const LOCKTAG *locktag,
												  LOCKMODE lockmode,
												  const ClusterGrdHolderId *requester,
												  uint32 *slot_idx_out);
extern void cluster_lms_native_probe_slot_release(uint32 slot_idx);
extern void cluster_lms_native_probe_dispatch(uint32 slot_idx);
extern void cluster_lms_native_probe_recv_reply(uint64 probe_id, int32 sender_node_id,
												ClusterNativeLockProbeReply status);
extern void cluster_lms_native_probe_aggregate_and_resolve(uint32 slot_idx);
extern void cluster_lms_native_probe_retry_tick(void);
extern void cluster_lms_native_probe_cleanup_on_node_dead(int32 dead_node_id);
extern void cluster_lms_native_probe_cleanup_on_backend_exit(int procno);
extern bool cluster_lms_native_probe_wait_clear(const ClusterResId *resid, LOCKMODE lockmode,
												const ClusterGrdHolderId *requester,
												int timeout_ms);
extern bool cluster_lms_native_probe_schedule_grant(const ClusterResId *resid, LOCKMODE lockmode,
													const ClusterGrdHolderId *requester,
													int32 source_node_id, uint32 request_opcode);

/* ============================================================
 * GUC defaults — actual values bound in cluster_guc.c (D9).
 * ============================================================ */
#define CLUSTER_LMS_NATIVE_LOCK_PROBE_MAX_INFLIGHT_DEFAULT 8
#define CLUSTER_LMS_NATIVE_LOCK_PROBE_RETRY_INTERVAL_MS_DEFAULT 500
#define CLUSTER_LMS_NATIVE_LOCK_PROBE_RETRY_BUDGET_DEFAULT 60

extern int cluster_lms_native_lock_probe_max_inflight;
extern int cluster_lms_native_lock_probe_retry_interval_ms;
extern int cluster_lms_native_lock_probe_retry_budget;

#endif /* !FRONTEND */

#endif /* CLUSTER_NATIVE_LOCK_PROBE_H */
