/*-------------------------------------------------------------------------
 *
 * cluster_oid_lease_shmem.c
 *	  Per-node OID lease shmem + cross-node refill (spec-6.14 D6).
 *
 *	  Under cluster.shared_catalog=on GetNewObjectId draws OIDs from this
 *	  node's lease block instead of the node-local ShmemVariableCache counter.
 *	  When the block is exhausted one backend on the node refills it from the
 *	  shared OID authority under a singleton cross-node X lock (GES), while
 *	  peers wait on a ConditionVariable and re-check -- mirroring the sequence
 *	  instance-cache refill (spec-5.4) and the CF singleton lock (spec-5.6).
 *	  The authority is fail-closed: an unreadable/corrupt authority raises
 *	  53RB (ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE); we never fall back
 *	  to the node-local counter.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_oid_lease_shmem.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D6)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"			/* IsUnderPostmaster */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_oid_lease.h"
#include "cluster/cluster_shmem.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * ClusterOidLeaseShared -- the node-level OID lease region.  next/end are the
 * live lease block; refill_in_progress + refill_cv serialise the (rare)
 * cross-node refill so only one backend drives it while peers wait.
 */
typedef struct ClusterOidLeaseShared
{
	LWLock		lwlock;			/* guards next/end/refill_in_progress */
	ConditionVariable refill_cv;	/* refill serialisation */
	Oid			next;			/* next OID this node may hand out */
	Oid			end;			/* exclusive block end (0 == top of space) */
	bool		refill_in_progress;
	pg_atomic_uint64 acquire_count;	/* OIDs handed out (D10) */
	pg_atomic_uint64 refill_count;	/* successful authority refills (D10) */
	pg_atomic_uint64 refill_wait_count;	/* times a backend waited on the CV */
} ClusterOidLeaseShared;

static ClusterOidLeaseShared *oid_state = NULL;

/*
 * Per-backend hold record for the singleton OID X lock (mirrors CfHoldState).
 */
typedef struct OidLeaseXHold
{
	bool		held;
	bool		coordinated;
	ClusterLockAcquireRequest req;
} OidLeaseXHold;

static OidLeaseXHold oid_x_hold;

/* ---- shmem region wiring ---------------------------------------------- */

Size
cluster_oid_lease_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterOidLeaseShared));
}

void
cluster_oid_lease_shmem_init(void)
{
	bool		found;

	oid_state = (ClusterOidLeaseShared *)
		ShmemInitStruct("pgrac cluster oid lease",
						MAXALIGN(sizeof(ClusterOidLeaseShared)), &found);

	if (!IsUnderPostmaster)
	{
		LWLockInitialize(&oid_state->lwlock, LWTRANCHE_CLUSTER_OID_LEASE);
		ConditionVariableInit(&oid_state->refill_cv);
		oid_state->next = InvalidOid;
		oid_state->end = InvalidOid;	/* next==end -> exhausted -> refill */
		oid_state->refill_in_progress = false;
		pg_atomic_init_u64(&oid_state->acquire_count, 0);
		pg_atomic_init_u64(&oid_state->refill_count, 0);
		pg_atomic_init_u64(&oid_state->refill_wait_count, 0);
	}
}

static const ClusterShmemRegion cluster_oid_lease_region = {
	.name = "pgrac cluster oid lease",
	.size_fn = cluster_oid_lease_shmem_size,
	.init_fn = cluster_oid_lease_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "spec-6.14 D6 OID lease",
};

void
cluster_oid_lease_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_oid_lease_region);
}

/* ---- singleton OID authority X lock (mirrors cluster_cf_lock) ---------- */

static bool
oid_authority_x_lock(void)
{
	ClusterLockAcquireRequest req;
	ClusterLockAcquireResult r;

	Assert(!oid_x_hold.held);

	memset(&req, 0, sizeof(req));
	cluster_oid_resid_encode(&req.resid);
	req.lockmode = ExclusiveLock;
	req.op = CLUSTER_LOCK_OP_REQUEST;
	req.current_mode = NoLock;
	req.lockmethod_id = DEFAULT_LOCKMETHOD;
	req.dontwait = false;
	req.sessionLock = false;
	req.caller_local_start_ts_ms = (uint64) (GetCurrentTimestamp() / 1000);
	req.timeout_ms = cluster_ges_request_timeout_ms;
	req.wait_event = WAIT_EVENT_CLUSTER_OID_LEASE;

	r = cluster_lock_acquire_seven_step(&req);

	switch (r)
	{
		case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:
			/* single-node / gate off: nothing registered in the GRD. */
			oid_x_hold.held = true;
			oid_x_hold.coordinated = false;
			oid_x_hold.req = req;
			return true;

		case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
		case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
		case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:
			/* granted at cluster level: promote the reservation to a holder. */
			if (cluster_lock_acquire_s5_promote(&req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
				return false;
			oid_x_hold.held = true;
			oid_x_hold.coordinated = true;
			oid_x_hold.req = req;
			return true;

		default:
			/* timeout / LMS unavailable / deadlock / internal: fail closed. */
			return false;
	}
}

static void
oid_authority_x_unlock(void)
{
	if (!oid_x_hold.held)
		return;
	if (oid_x_hold.coordinated)
		(void) cluster_lock_acquire_s6_release(&oid_x_hold.req);
	oid_x_hold.held = false;
	oid_x_hold.coordinated = false;
}

/*
 * refill_from_authority -- drive one cross-node refill.  Acquires the OID X
 * lock, reads the shared high-water, carves this node's next block, writes the
 * advanced high-water back durably, releases the X lock.  Returns the new
 * block via *out_start / *out_end.  Raises 53RB (fail-closed) when the
 * authority is unavailable; no node-local fallback.  Holds NO node-local
 * LWLock across the GES round trip.
 */
static void
refill_from_authority(Oid *out_start, Oid *out_end)
{
	Oid			hw;
	Oid			new_authority;

	if (!oid_authority_x_lock())
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("could not acquire the shared OID authority lock"),
				 errhint("The cluster OID authority is unreachable; retry once the "
						 "cluster is healthy.")));

	PG_TRY();
	{
		if (!cluster_oid_authority_read(&hw))
			ereport(ERROR,
					(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
					 errmsg("shared OID authority is unavailable or corrupt"),
					 errhint("Check cluster.shared_data_dir and the "
							 "global/pgrac_oid_authority file.")));

		cluster_oid_lease_carve(hw, (uint32) cluster_oid_lease_size,
								out_start, out_end, &new_authority);

		/* Advance the durable high-water BEFORE handing out the block. */
		cluster_oid_authority_write(new_authority);
	}
	PG_FINALLY();
	{
		oid_authority_x_unlock();
	}
	PG_END_TRY();

	pg_atomic_fetch_add_u64(&oid_state->refill_count, 1);
}

/*
 * cluster_oid_lease_get_next -- see header.  Fast path: consume one OID from
 * the live block under the node LWLock.  Slow path: coordinate a single
 * cross-node refill via refill_in_progress + the CV, then retry.
 */
Oid
cluster_oid_lease_get_next(void)
{
	Assert(oid_state != NULL);

	for (;;)
	{
		Oid			oid;
		Oid			new_start;
		Oid			new_end;

		LWLockAcquire(&oid_state->lwlock, LW_EXCLUSIVE);

		/* Fast path: the live block still has an OID. */
		if (oid_state->next != oid_state->end)
		{
			ClusterOidLease live;

			live.next = oid_state->next;
			live.end = oid_state->end;
			oid = cluster_oid_lease_consume(&live);
			oid_state->next = live.next;
			LWLockRelease(&oid_state->lwlock);

			Assert(OidIsValid(oid));
			pg_atomic_fetch_add_u64(&oid_state->acquire_count, 1);
			return oid;
		}

		/* Exhausted.  If someone else is refilling, wait and re-check. */
		if (oid_state->refill_in_progress)
		{
			ConditionVariablePrepareToSleep(&oid_state->refill_cv);
			LWLockRelease(&oid_state->lwlock);
			pg_atomic_fetch_add_u64(&oid_state->refill_wait_count, 1);
			ConditionVariableSleep(&oid_state->refill_cv, WAIT_EVENT_CLUSTER_OID_LEASE);
			ConditionVariableCancelSleep();
			continue;
		}

		/* Claim the refill; drive it with NO node LWLock held. */
		oid_state->refill_in_progress = true;
		LWLockRelease(&oid_state->lwlock);

		PG_TRY();
		{
			refill_from_authority(&new_start, &new_end);
		}
		PG_CATCH();
		{
			/* Fail-closed: clear the claim + wake waiters so they re-check
			 * (and hit the same authority error, or a later success). */
			LWLockAcquire(&oid_state->lwlock, LW_EXCLUSIVE);
			oid_state->refill_in_progress = false;
			LWLockRelease(&oid_state->lwlock);
			ConditionVariableBroadcast(&oid_state->refill_cv);
			PG_RE_THROW();
		}
		PG_END_TRY();

		/* Install the new block and wake waiters. */
		LWLockAcquire(&oid_state->lwlock, LW_EXCLUSIVE);
		oid_state->next = new_start;
		oid_state->end = new_end;
		oid_state->refill_in_progress = false;
		LWLockRelease(&oid_state->lwlock);
		ConditionVariableBroadcast(&oid_state->refill_cv);
		/* loop: the next iteration consumes from the fresh block */
	}
}

/* ---- observability (D10) ---------------------------------------------- */

uint64
cluster_oid_lease_acquire_count(void)
{
	return oid_state ? pg_atomic_read_u64(&oid_state->acquire_count) : 0;
}

/*
 * cluster_oid_lease_remaining -- OIDs left in the current lease block (0 when
 * the block runs to the top of the OID space or is exhausted).  Best-effort
 * snapshot for observability; not locked.
 */
Oid
cluster_oid_lease_remaining(void)
{
	Oid			n,
				e;

	if (oid_state == NULL)
		return 0;
	n = oid_state->next;
	e = oid_state->end;
	if (e == 0)					/* block runs to top of space */
		return 0;
	return (e >= n) ? (e - n) : 0;
}
