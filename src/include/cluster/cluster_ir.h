/*-------------------------------------------------------------------------
 *
 * cluster_ir.h
 *	  IR (instance-recovery owner) cross-node enqueue lock -- spec-5.7 §3.4 / D8.
 *
 *	  When a node dies, the survivors online-recover its WAL data + visibility
 *	  (the spec-4.11 thread-recovery worker).  The destructive part of that
 *	  recovery (physical rollback / cleanup) is NOT idempotent, so it must run on
 *	  EXACTLY ONE survivor.  Today uniqueness rests on three local-view facts:
 *	  the deterministic min(survivor) coordinator, the per-episode epoch, and the
 *	  idempotence of redo.  But two survivors whose alive-set views DIVERGE could
 *	  each believe they are the min(survivor) owner and both mutate the same dead
 *	  resource -> double destructive apply (8.A).  IR(X) adds a GES-ENFORCED layer:
 *	  the recovery worker takes IR(X) on (dead_node, episode_epoch) before any
 *	  destructive apply, so the GES master grants exactly one owner; a non-owner
 *	  fails closed (53RA9) and never mutates.
 *
 *	  Bootstrap phase (IR-M5, P1#7): IR(X) is acquired only AFTER the reconfig
 *	  epoch is accepted (so the resid carries the live episode) -- and it must
 *	  NEVER block the remaster machinery it depends on.  The recovery worker runs
 *	  inside the freeze window, while the IR resid's GRD shard is still REBUILDING
 *	  and the shard-freeze gate (cluster_lock_acquire.c) would otherwise reject a
 *	  grant.  But a (dead_node, NEW_epoch) resid is BRAND NEW this episode: no node
 *	  has ever held IR(X) on it, so there is no holder set being rebuilt and the
 *	  freeze gate's only purpose -- not granting against a half-rebuilt holder set
 *	  -- is vacuous for it.  So the IR acquire sets ClusterLockAcquireRequest.
 *	  recovery_bootstrap, which lets the requester-side freeze gate be bypassed for
 *	  a fresh-epoch IR resid.  The normal grant/conflict path is unchanged, so the
 *	  cross-survivor mutual exclusion still holds; only the phase check is skipped.
 *	  (The master-side freeze check in cluster_ges.c is the >2-node forward piece:
 *	  online thread recovery is 2-node-scoped today, where the sole survivor
 *	  self-masters the IR resid, so the requester-side bypass is sufficient.)
 *
 *	  Epoch staleness (IR-M2): a new reconfig episode = a new epoch = a new resid,
 *	  so an old owner's lock is naturally distinct and the worker's existing L235
 *	  superseded-epoch abort (cluster_thread_recovery_worker.c) still applies on
 *	  top.  IR uses dontwait semantics: a competitor that already holds IR(X) is
 *	  reported immediately as NOT_OWNER (the worker never waits) -- so IR can never
 *	  block the remaster or stall the freeze window.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_ir.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D8, §3.4)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_IR_H
#define CLUSTER_IR_H

#include "cluster/cluster_grd.h" /* ClusterResId */
#include "cluster/cluster_hw.h"	 /* CLUSTER_HW_RESID_TYPE (collision check) */
#include "cluster/cluster_dl.h"	 /* CLUSTER_DL_RESID_TYPE (collision check) */
#include "storage/lock.h"		 /* LOCKTAG_LAST_TYPE */

/*
 * CLUSTER_IR_RESID_TYPE -- IR resource-id namespace marker.  Above every PG
 * LockTagType, distinct from SQ (0xF0) / CF (0xF1) / HW (0xF2) / DL (0xF3) /
 * TT (0xF4).
 */
#define CLUSTER_IR_RESID_TYPE 0xF5

StaticAssertDecl(CLUSTER_IR_RESID_TYPE > LOCKTAG_LAST_TYPE,
				 "IR resid namespace must not collide with any PG LockTagType");
StaticAssertDecl(CLUSTER_IR_RESID_TYPE != CLUSTER_HW_RESID_TYPE,
				 "IR and HW resid namespaces must be distinct");
StaticAssertDecl(CLUSTER_IR_RESID_TYPE != CLUSTER_DL_RESID_TYPE,
				 "IR and DL resid namespaces must be distinct");
/* The other siblings -- SQ (0xF0), CF (0xF1, spec-5.6), TT (0xF4, a later
 * spec-5.7 deliverable) -- are defined in headers this one does not pull in, but
 * 0xF5 is arithmetically distinct from each.  test_cluster_ir asserts the full
 * SQ/CF/HW/DL distinctness at unit-test link time (where those headers ARE
 * included), so the collision contract is checked end-to-end. */

/* ---- pure layer (standalone-linkable; never ereports) --------------- */

/*
 * cluster_ir_resid_encode -- build the IR resource id for one dead node's
 * recovery in one reconfig episode.  field1 = dead_node_id, field2 = the low 32
 * bits and field3 = the high 32 bits of the episode epoch (the full 64-bit epoch
 * so a new episode is a distinct resid -- IR-M2), field4 = 0.  Two survivors
 * recovering the same dead node in the same episode hash to the same resid and
 * compete for IR(X); the GES master grants exactly one.
 */
extern void cluster_ir_resid_encode(int32 dead_node_id, uint64 episode_epoch, ClusterResId *dst);

/*
 * cluster_ir_bootstrap_ready -- pure IR-M5 precondition (testable without the
 * GES).  IR(X) may be acquired only when the episode epoch the worker launched
 * under is the CURRENT accepted epoch (and is a real episode, != 0).  A stale
 * launch epoch means the reconfig advanced past this worker -> defer to the new
 * episode (the worker's L235 superseded-epoch abort is the runtime enforcer; this
 * predicate documents and unit-checks the same gate).
 */
extern bool cluster_ir_bootstrap_ready(uint64 episode_epoch, uint64 accepted_epoch);

#ifndef FRONTEND

#include "cluster/cluster_lock_acquire.h" /* ClusterLockAcquireRequest */

/*
 * ClusterIrLock -- a held (or evaluated) IR(X) ownership claim.  coordinated ==
 * false means the GES resolved this to a native/local outcome (no cross-node
 * competitor exists) -- release is a no-op.
 */
typedef struct ClusterIrLock {
	bool held;
	bool coordinated;
	ClusterResId resid;
	ClusterLockAcquireRequest req; /* for the S6 release */
} ClusterIrLock;

/*
 * ClusterIrAcquireOutcome -- the result of competing for recovery ownership.
 *
 *	OWNER      IR(X) granted: this node is the sole recovery owner -> it MUST do
 *	           the destructive apply, then release.  lk->held == true.
 *	NATIVE     the cluster/LMS layer resolved this to a native/local outcome (no
 *	           cross-node competitor can exist) -> proceed under the existing
 *	           min(survivor)+epoch authority.  lk->held == false.
 *	NOT_OWNER  a peer holds IR(X) for this (dead_node, epoch) -> this node is NOT
 *	           the owner (view divergence): FAIL CLOSED 53RA9, never mutate.
 *	NOT_READY  the bootstrap precondition is unmet (epoch not accepted) or the
 *	           ownership could not be determined (LMS unavailable / transient) ->
 *	           do not mutate this tick; keep frozen (8.A), retry next episode.
 */
typedef enum ClusterIrAcquireOutcome {
	CLUSTER_IR_OWNER = 0,
	CLUSTER_IR_NATIVE,
	CLUSTER_IR_NOT_OWNER,
	CLUSTER_IR_NOT_READY,
} ClusterIrAcquireOutcome;

/*
 * cluster_ir_recovery_acquire -- §3.4 IR-M1/IR-M3/IR-M5: compete for IR(X) on
 * (dead_node_id, episode_epoch) before a destructive recovery apply.  Fills *lk.
 * Honours the bootstrap phase (epoch accepted) and never blocks (dontwait +
 * fresh-epoch freeze-gate bypass), so it cannot stall the remaster it depends on.
 * See ClusterIrAcquireOutcome for the contract on each return.
 */
extern ClusterIrAcquireOutcome cluster_ir_recovery_acquire(int32 dead_node_id, uint64 episode_epoch,
														   ClusterIrLock *lk);

/*
 * cluster_ir_recovery_release -- release a held IR(X) ownership claim (if any).
 * A no-op when nothing coordinated was held.  Idempotent.
 */
extern void cluster_ir_recovery_release(ClusterIrLock *lk);

/* Minimal shmem region (four counters); mirror cluster_dl_shmem_*. */
extern Size cluster_ir_shmem_size(void);
extern void cluster_ir_shmem_init(void);
extern void cluster_ir_shmem_register(void);

/* Observability counters (dump_ir; surfaced by pg_cluster_state). */
extern uint64 cluster_ir_owner_count(void);	   /* IR(X) granted (this node = owner) */
extern uint64 cluster_ir_native_count(void);   /* uncoordinated / native proceed */
extern uint64 cluster_ir_conflict_count(void); /* 53RA9 non-owner fail-closed */
extern uint64 cluster_ir_release_count(void);  /* IR(X) ownership claims released */

#endif /* !FRONTEND */

#endif /* CLUSTER_IR_H */
