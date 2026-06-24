/*-------------------------------------------------------------------------
 *
 * cluster_ir_lock.c
 *	  IR (instance-recovery owner) backend: shmem counters + GES acquire/release
 *	  for the destructive thread-recovery mutation gate (spec-5.7 §3.4 / D8).
 *	  The pure resid encoder + bootstrap predicate live in cluster_ir.c
 *	  (standalone-linkable for the unit test).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ir_lock.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D8, §3.4)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"  /* cluster_node_id / cluster_conf_node_count */
#include "cluster/cluster_epoch.h" /* cluster_epoch_get_current (bootstrap gate) */
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h" /* cluster_ges_request_timeout_ms */
#include "cluster/cluster_ir.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h" /* IsUnderPostmaster */
#include "storage/lock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "port/atomics.h"

/* ============================================================
 * Shmem region: four observability counters.
 * ============================================================ */

typedef struct ClusterIrShared {
	pg_atomic_uint64 owner_count;	 /* IR(X) granted: this node = recovery owner */
	pg_atomic_uint64 native_count;	 /* uncoordinated / native proceed */
	pg_atomic_uint64 conflict_count; /* 53RA9 non-owner fail-closed */
	pg_atomic_uint64 release_count;	 /* IR(X) ownership claims released */
} ClusterIrShared;

static ClusterIrShared *ir_state = NULL;

Size
cluster_ir_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterIrShared));
}

void
cluster_ir_shmem_init(void)
{
	bool found;

	ir_state = (ClusterIrShared *)ShmemInitStruct("pgrac cluster ir",
												  MAXALIGN(sizeof(ClusterIrShared)), &found);
	if (!IsUnderPostmaster) {
		pg_atomic_init_u64(&ir_state->owner_count, 0);
		pg_atomic_init_u64(&ir_state->native_count, 0);
		pg_atomic_init_u64(&ir_state->conflict_count, 0);
		pg_atomic_init_u64(&ir_state->release_count, 0);
	}
}

static const ClusterShmemRegion cluster_ir_region = {
	.name = "pgrac cluster ir",
	.size_fn = cluster_ir_shmem_size,
	.init_fn = cluster_ir_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "spec-5.7 IR instance-recovery owner",
	.reserved_flags = 0,
};

void
cluster_ir_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ir_region);
}

#define IR_BUMP(field)                                                                             \
	do {                                                                                           \
		if (ir_state != NULL)                                                                      \
			pg_atomic_fetch_add_u64(&ir_state->field, 1);                                          \
	} while (0)

uint64
cluster_ir_owner_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->owner_count) : 0;
}
uint64
cluster_ir_native_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->native_count) : 0;
}
uint64
cluster_ir_conflict_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->conflict_count) : 0;
}
uint64
cluster_ir_release_count(void)
{
	return ir_state != NULL ? pg_atomic_read_u64(&ir_state->release_count) : 0;
}


/* ============================================================
 * GES acquire / release over the spec-5.3 substrate.
 * ============================================================ */

ClusterIrAcquireOutcome
cluster_ir_recovery_acquire(int32 dead_node_id, uint64 episode_epoch, ClusterIrLock *lk)
{
	ClusterResId resid;
	ClusterLockAcquireRequest req;
	ClusterLockAcquireResult r;

	Assert(lk != NULL);
	if (lk == NULL)
		return CLUSTER_IR_NOT_READY;
	memset(lk, 0, sizeof(*lk));

	/*
	 * IR enforcement only matters in a live multi-node cluster: with no peers there
	 * is no competing survivor, so the existing min(survivor)+epoch authority is
	 * sufficient and the worker proceeds (this is the single-node / cluster-off
	 * path, mirroring how DL/HW back off).  Returning NATIVE keeps the single-node
	 * recovery path -- including the t/265 in-process driver -- unchanged.
	 */
	if (cluster_node_id < 0 || cluster_conf_node_count() <= 1) {
		IR_BUMP(native_count);
		return CLUSTER_IR_NATIVE;
	}

	/*
	 * IR-M5 bootstrap precondition: the launch episode epoch must be the current
	 * accepted epoch.  A mismatch means the reconfig advanced past this worker ->
	 * do not acquire (the worker's L235 superseded-epoch abort is the runtime
	 * enforcer; this is the additional IR gate).  Never block the remaster.
	 */
	if (!cluster_ir_bootstrap_ready(episode_epoch, cluster_epoch_get_current()))
		return CLUSTER_IR_NOT_READY;

	cluster_ir_resid_encode(dead_node_id, episode_epoch, &resid);

	memset(&req, 0, sizeof(req));
	req.resid = resid;
	req.lockmode = ExclusiveLock;
	req.op = CLUSTER_LOCK_OP_REQUEST; /* IR never converts (new epoch = new resid) */
	req.current_mode = NoLock;
	req.lockmethod_id = DEFAULT_LOCKMETHOD;
	/*
	 * dontwait (IR-M5): a peer that already holds IR(X) means this node is NOT the
	 * recovery owner -- report it immediately (NOT_OWNER -> 53RA9) rather than
	 * wait.  IR must never block, so the freeze window / remaster is never stalled.
	 */
	req.dontwait = true;
	req.sessionLock = false;
	/*
	 * recovery_bootstrap (the fresh-epoch freeze-gate bypass): the recovery worker
	 * runs inside the freeze window while the IR resid's GRD shard is still
	 * REBUILDING.  A (dead_node, NEW_epoch) resid is brand new this episode -- no
	 * holder set is being rebuilt for it -- so the requester-side shard-freeze gate
	 * (cluster_lock_acquire.c) is skipped for it.  The normal grant/conflict path
	 * is unchanged, so cross-survivor mutual exclusion still holds.
	 */
	req.recovery_bootstrap = true;
	req.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	req.timeout_ms = cluster_ges_request_timeout_ms;
	/* Per spec-5.7 Q10, IR reuses the existing GES request wait event. */
	req.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;

	r = cluster_lock_acquire_seven_step(&req);

	switch (r) {
	case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:
		/*
		 * The cluster/LMS layer resolved this to a native/local outcome (single
		 * node / cluster coordination off): no cross-node competitor can exist, so
		 * the existing min(survivor)+epoch authority is sufficient -- proceed.
		 */
		IR_BUMP(native_count);
		return CLUSTER_IR_NATIVE;

	case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
		/*
		 * S3 reservation succeeded; IR has no PG-native heavyweight lock to take in
		 * between (mirror DL), so promote now to register the GRD holder.  A promote
		 * failure cannot prove sole ownership -> fail-closed NOT_READY (keep frozen).
		 */
		if (cluster_lock_acquire_s5_promote(&req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return CLUSTER_IR_NOT_READY;
		lk->held = true;
		lk->coordinated = true;
		lk->resid = resid;
		lk->req = req;
		IR_BUMP(owner_count);
		return CLUSTER_IR_OWNER;

	case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
	case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:
		/*
		 * Legacy / stub S4 path: seven_step already ran S5 internally and returned
		 * an already-promoted holder.  Do NOT promote a second time.  Record it.
		 */
		lk->held = true;
		lk->coordinated = true;
		lk->resid = resid;
		lk->req = req;
		IR_BUMP(owner_count);
		return CLUSTER_IR_OWNER;

	case CLUSTER_LOCK_ACQUIRE_NOT_AVAIL:
		/*
		 * A peer holds IR(X) for this (dead_node, episode_epoch): this node is NOT
		 * the recovery owner -- the destructive apply belongs to the holder.  Fail
		 * closed (the worker maps this to 53RA9 and never mutates).
		 */
		IR_BUMP(conflict_count);
		return CLUSTER_IR_NOT_OWNER;

	default:
		/*
		 * Anything else (LMS unavailable / timeout / GRD-not-ready / shard
		 * remastering past budget / internal): ownership cannot be proven, so do
		 * NOT mutate -- keep the dead origin frozen (8.A) and let a later episode
		 * retry.  Distinct from NOT_OWNER: no peer is known to own it.
		 */
		return CLUSTER_IR_NOT_READY;
	}
}

void
cluster_ir_recovery_release(ClusterIrLock *lk)
{
	if (lk == NULL || !lk->held)
		return;
	if (lk->coordinated) {
		(void)cluster_lock_acquire_s6_release(&lk->req);
		IR_BUMP(release_count);
	}
	lk->held = false;
	lk->coordinated = false;
}
