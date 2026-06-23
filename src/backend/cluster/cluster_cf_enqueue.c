/*-------------------------------------------------------------------------
 *
 * cluster_cf_enqueue.c
 *	  CF (control file) cluster enqueue over the spec-5.3 GES substrate
 *	  (spec-5.6 Db1/Db2).
 *
 *	  Db1 (this stage): the singleton CF resource-id encoder.  Db2 adds the
 *	  cluster_cf_lock/unlock acquire/release wrappers that build a
 *	  ClusterLockAcquireRequest for the CF resid and drive the GES seven-
 *	  step state machine.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cf_enqueue.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.6-cf-enqueue-shared-controlfile-authority.md (Db1/Db2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lock_acquire.h"
#include "storage/lock.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * CfHoldState -- per-backend record of a held CF lock so the matching
 * release can target the exact GRD holder + request_id the acquire
 * registered.  `coordinated` is false when the acquire returned OK_NATIVE
 * (cluster/LMS layer inactive): nothing was registered in the GRD, so the
 * release must not run S6 against a phantom holder.  There is one slot per
 * mode (X / S); CF is not reentrant within a backend for a given mode.
 */
typedef struct CfHoldState {
	bool held;
	bool coordinated;
	ClusterLockAcquireRequest req; /* resid + holder + request_id for release */
} CfHoldState;

static CfHoldState cf_hold_x;
static CfHoldState cf_hold_s;

/*
 * spec-5.6 Db5: set while this process is the bootstrap single-node authority
 * (sole-liveness + storage contract proven, GES not yet ready).  Lets the
 * write path proceed without a held CF X during early recovery.
 */
static bool cf_bootstrap_authority = false;

/*
 * spec-5.6 increment (ii/iii): JOIN_READONLY marks an attaching (join) node
 * recovering against a live peer that owns the shared authority.  Unlike the
 * single-node OWNER window above (process-local: only the startup process
 * writes the authority during single-node bring-up), the join role must be
 * visible CROSS-PROCESS -- the checkpointer's end-of-recovery checkpoint must
 * see it to skip CF X before GES is ready -- so it is backed by the lock-free
 * CF shmem flag (cluster_cf_stats_*), not a process-local static.
 */

static CfHoldState *
cf_slot(LOCKMODE mode)
{
	Assert(mode == ShareLock || mode == ExclusiveLock);
	return (mode == ExclusiveLock) ? &cf_hold_x : &cf_hold_s;
}

/*
 * cluster_cf_resid_encode -- build the singleton CF resource id.
 *
 *	CF is one whole-file lock, so every map field is zero; only the type
 *	byte (CLUSTER_CF_RESID_TYPE) places it in the CF namespace.  The
 *	lockmethodid mirrors the SQ encoder (DEFAULT_LOCKMETHOD) so the GES
 *	routing hash treats it uniformly.
 */
void
cluster_cf_resid_encode(ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = 0;
	dst->field2 = 0;
	dst->field3 = 0;
	dst->field4 = 0;
	dst->type = CLUSTER_CF_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

/*
 * cluster_cf_lock -- acquire the singleton CF lock in `mode` via GES.
 */
bool
cluster_cf_lock(LOCKMODE mode)
{
	ClusterLockAcquireRequest req;
	ClusterLockAcquireResult r;
	CfHoldState *slot = cf_slot(mode);

	/* CF is not reentrant within a backend (one caller-level acquire). */
	Assert(!slot->held);

	memset(&req, 0, sizeof(req));
	cluster_cf_resid_encode(&req.resid);
	/* locktag left zeroed: not LOCKTAG_ADVISORY, so normal blocking semantics. */
	req.lockmode = mode;
	req.op = CLUSTER_LOCK_OP_REQUEST; /* CF never converts (X/S independent) */
	req.current_mode = NoLock;
	req.lockmethod_id = DEFAULT_LOCKMETHOD;
	req.dontwait = false; /* block until granted or timeout */
	req.sessionLock = false;
	req.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	/* spec-5.6 Dc4b: bound the CF acquire wait and label it ClusterCfEnqueueWait. */
	req.timeout_ms = cluster_cf_enqueue_timeout_ms;
	req.wait_event = WAIT_EVENT_CLUSTER_CF_ENQUEUE;

	r = cluster_lock_acquire_seven_step(&req);

	switch (r) {
	case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:

		/*
			 * Cluster/LMS layer inactive (single-node or gate off): no cross-
			 * node coordination is needed and nothing was registered in the
			 * GRD.  Treat as held but uncoordinated so release is a no-op.
			 */
		slot->held = true;
		slot->coordinated = false;
		slot->req = req;
		cluster_cf_counter_inc(mode == ExclusiveLock ? CLUSTER_CF_X_ACQUIRE : CLUSTER_CF_S_ACQUIRE);
		return true;

	case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
	case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
	case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:

		/*
			 * Granted at the cluster level.  CF has no PG-native heavyweight
			 * lock to take, so run the S5 promote directly to turn the S3
			 * reservation into a registered GRD holder (cross-node conflict
			 * visibility).  S5 failure cancels the reservation (S7) and we
			 * fail closed.
			 */
		if (cluster_lock_acquire_s5_promote(&req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED) {
			cluster_cf_counter_inc(CLUSTER_CF_FAILCLOSED);
			return false;
		}
		slot->held = true;
		slot->coordinated = true;
		slot->req = req; /* holder + request_id for the release */
		cluster_cf_counter_inc(mode == ExclusiveLock ? CLUSTER_CF_X_ACQUIRE : CLUSTER_CF_S_ACQUIRE);
		return true;

	default:

		/*
			 * NOT_AVAIL (try conflict; CF blocks so this is unexpected),
			 * timeout, LMS-unavailable, deadlock, internal, etc.  The lock
			 * could not be proven held: fail closed.  The caller raises the
			 * appropriate FATAL/ERROR (CF correctness).
			 */
		cluster_cf_counter_inc(CLUSTER_CF_FAILCLOSED);
		return false;
	}
}

/*
 * cluster_cf_unlock -- release a previously-held CF lock in `mode`.
 */
void
cluster_cf_unlock(LOCKMODE mode)
{
	CfHoldState *slot = cf_slot(mode);

	if (!slot->held)
		return;

	/*
	 * Release the exact GRD holder the acquire registered, draining + waking
	 * any blocked cross-node waiters (S6).  Use the captured request (CF
	 * resid + holder + request_id) rather than cluster_lock_release(), which
	 * re-derives the resid from a PG LOCKTAG that CF does not have.
	 */
	if (slot->coordinated)
		(void)cluster_lock_acquire_s6_release(&slot->req);

	slot->held = false;
	slot->coordinated = false;
}

/*
 * cluster_cf_held -- does this backend hold the CF lock in `mode`?
 */
bool
cluster_cf_held(LOCKMODE mode)
{
	return cf_slot(mode)->held;
}

/*
 * cluster_cf_set_bootstrap_authority -- mark/clear the bootstrap window.
 */
void
cluster_cf_set_bootstrap_authority(bool on)
{
	cf_bootstrap_authority = on;
}

/*
 * cluster_cf_write_permitted -- is a shared-authority control-file write
 * currently allowed (held CF X, or the bootstrap single-node window)?
 */
bool
cluster_cf_write_permitted(void)
{
	return cluster_cf_held(ExclusiveLock) || cf_bootstrap_authority;
}

/*
 * cluster_cf_in_bootstrap_window -- is this process the bootstrap authority?
 */
bool
cluster_cf_in_bootstrap_window(void)
{
	return cf_bootstrap_authority;
}

/*
 * cluster_cf_set_join_readonly -- mark/clear this NODE as a join-read-only
 * bring-up node (cross-process via the CF shmem flag).
 */
void
cluster_cf_set_join_readonly(bool on)
{
	cluster_cf_stats_set_join_readonly(on);
}

/*
 * cluster_cf_join_readonly -- is this node attaching read-only to a peer-owned
 * authority (recovery writes must be skipped, not applied)?  Cross-process:
 * the checkpointer reads the flag the startup process set.
 */
bool
cluster_cf_join_readonly(void)
{
	return cluster_cf_stats_get_join_readonly();
}

/*
 * cf_write_skip -- PROCESS-LOCAL bring-up authority-write-skip (see header).
 * Distinct from the node-wide shmem join_readonly: the chokepoint consults
 * ONLY this, so a lingering shmem flag cannot silently skip a steady-state
 * write from another path.
 */
static bool cf_write_skip = false;

void
cluster_cf_set_write_skip(bool on)
{
	cf_write_skip = on;
}

bool
cluster_cf_write_skip(void)
{
	return cf_write_skip;
}
