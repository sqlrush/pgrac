/*-------------------------------------------------------------------------
 *
 * cluster_relmap_lock.c
 *	  Singleton cross-node relmap-authority X lock (spec-6.14 D5-activation).
 *
 *	  One synthetic GES resource (type 0xF8, all map fields zero) serialises
 *	  every relation-map authority writer in the cluster.  The hold record is
 *	  per-backend and survives transaction commit on purpose: the on-mode
 *	  write path acquires the lock pre-commit (before staging the pending
 *	  image) and releases it only after the post-commit publish + cross-node
 *	  invalidation ack round (spec §3.2 -- AD-001 order "write file ->
 *	  broadcast -> ACK -> drop -> unlock").  A pre-commit abort releases it
 *	  through cluster_relmap_lock_abort_release (xact.c); a post-commit
 *	  failure PANICs (r4-P1), so the lock dies with the node and reconfig +
 *	  crash arbitration take over.
 *
 *	  Mirrors the OID-authority singleton lock (cluster_oid_lease_shmem.c)
 *	  and the CF singleton lock (spec-5.6).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_relmap_lock.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D5, §3.2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_relmap_lock.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * Per-backend hold record for the singleton relmap X lock (mirrors
 * OidLeaseXHold / CfHoldState).
 */
typedef struct RelmapXHold {
	bool held;
	bool coordinated;
	ClusterLockAcquireRequest req;
} RelmapXHold;

static RelmapXHold relmap_x_hold;

void
cluster_relmap_resid_encode(ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = 0;
	dst->field2 = 0;
	dst->field3 = 0;
	dst->field4 = 0;
	dst->type = CLUSTER_RELMAP_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

bool
cluster_relmap_authority_x_lock(void)
{
	ClusterLockAcquireRequest req;
	ClusterLockAcquireResult r;

	Assert(!relmap_x_hold.held);

	memset(&req, 0, sizeof(req));
	cluster_relmap_resid_encode(&req.resid);
	req.lockmode = ExclusiveLock;
	req.op = CLUSTER_LOCK_OP_REQUEST;
	req.current_mode = NoLock;
	req.lockmethod_id = DEFAULT_LOCKMETHOD;
	req.dontwait = false;
	req.sessionLock = false;
	req.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	req.timeout_ms = cluster_ges_request_timeout_ms;
	req.wait_event = WAIT_EVENT_CLUSTER_RELMAP_WRITE;

	r = cluster_lock_acquire_seven_step(&req);

	switch (r) {
	case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:
		/* single-node / gate off: nothing registered in the GRD. */
		relmap_x_hold.held = true;
		relmap_x_hold.coordinated = false;
		relmap_x_hold.req = req;
		return true;

	case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
	case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
	case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:
		/* granted at cluster level: promote the reservation to a holder. */
		if (cluster_lock_acquire_s5_promote(&req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return false;
		relmap_x_hold.held = true;
		relmap_x_hold.coordinated = true;
		relmap_x_hold.req = req;
		return true;

	default:
		/* timeout / LMS unavailable / deadlock / internal: fail closed. */
		return false;
	}
}

void
cluster_relmap_authority_x_unlock(void)
{
	if (!relmap_x_hold.held)
		return;
	if (relmap_x_hold.coordinated)
		(void)cluster_lock_acquire_s6_release(&relmap_x_hold.req);
	relmap_x_hold.held = false;
	relmap_x_hold.coordinated = false;
}

bool
cluster_relmap_authority_x_held(void)
{
	return relmap_x_hold.held;
}

void
cluster_relmap_lock_abort_release(void)
{
	cluster_relmap_authority_x_unlock();
}
