/*-------------------------------------------------------------------------
 *
 * cluster_ts_lock.c
 *	  TT (tablespace-DDL) backend: shmem counters + GES acquire + top-xact-end
 *	  release (spec-5.7 §3.3 / D5).  The pure resid encoders live in cluster_ts.c
 *	  (standalone-linkable for the unit test).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ts_lock.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D5, §3.3)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h" /* IsTransactionState, TopTransactionContext */
#include "access/xlog.h" /* RecoveryInProgress */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_ts.h"
#include "miscadmin.h"
#include "storage/lock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "port/atomics.h"

/* ============================================================
 * Shmem region: four observability counters.
 * ============================================================ */

typedef struct ClusterTsShared {
	pg_atomic_uint64 x_count;		   /* TT(X) DDL locks granted (coordinated) */
	pg_atomic_uint64 s_count;		   /* TT(S) placement locks granted (coordinated) */
	pg_atomic_uint64 native_count;	   /* uncoordinated / native proceed */
	pg_atomic_uint64 failclosed_count; /* 53RA8 fail-closed */
} ClusterTsShared;

static ClusterTsShared *ts_state = NULL;

Size
cluster_ts_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterTsShared));
}

void
cluster_ts_shmem_init(void)
{
	bool found;

	ts_state = (ClusterTsShared *)ShmemInitStruct("pgrac cluster ts",
												  MAXALIGN(sizeof(ClusterTsShared)), &found);
	if (!IsUnderPostmaster) {
		pg_atomic_init_u64(&ts_state->x_count, 0);
		pg_atomic_init_u64(&ts_state->s_count, 0);
		pg_atomic_init_u64(&ts_state->native_count, 0);
		pg_atomic_init_u64(&ts_state->failclosed_count, 0);
	}
}

static const ClusterShmemRegion cluster_ts_region = {
	.name = "pgrac cluster ts",
	.size_fn = cluster_ts_shmem_size,
	.init_fn = cluster_ts_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "spec-5.7 TT tablespace-DDL lock",
	.reserved_flags = 0,
};

void
cluster_ts_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ts_region);
}

#define TS_BUMP(field)                                                                             \
	do {                                                                                           \
		if (ts_state != NULL)                                                                      \
			pg_atomic_fetch_add_u64(&ts_state->field, 1);                                          \
	} while (0)

uint64
cluster_ts_x_count(void)
{
	return ts_state != NULL ? pg_atomic_read_u64(&ts_state->x_count) : 0;
}
uint64
cluster_ts_s_count(void)
{
	return ts_state != NULL ? pg_atomic_read_u64(&ts_state->s_count) : 0;
}
uint64
cluster_ts_native_count(void)
{
	return ts_state != NULL ? pg_atomic_read_u64(&ts_state->native_count) : 0;
}
uint64
cluster_ts_failclosed_count(void)
{
	return ts_state != NULL ? pg_atomic_read_u64(&ts_state->failclosed_count) : 0;
}


/* ============================================================
 * GES acquire / release over the spec-5.3 substrate.
 * ============================================================ */

typedef enum TsAcquireOutcome {
	TS_ACQUIRE_GRANTED = 0,
	TS_ACQUIRE_NATIVE,
	TS_ACQUIRE_FAILED,
} TsAcquireOutcome;

/*
 * ts_lock -- acquire TT(`mode`) on `resid` over the GES substrate.  NOWAIT
 * (dontwait): tablespace DDL is rare and admin-driven, and 53RA8 is the spec's
 * "TT conflict" code -- a conflicting holder fails fast (retryable) rather than
 * queueing the whole cluster's DDL.  Fills *lk; returns GRANTED / NATIVE (cluster
 * off -> local catalog locks suffice) / FAILED (caller fails closed 53RA8).
 */
static TsAcquireOutcome
ts_lock(const ClusterResId *resid, LOCKMODE mode, ClusterTsLock *lk)
{
	ClusterLockAcquireRequest req;
	ClusterLockAcquireResult r;

	Assert(resid != NULL && lk != NULL);
	memset(lk, 0, sizeof(*lk));

	memset(&req, 0, sizeof(req));
	req.resid = *resid;
	req.lockmode = mode;
	req.op = CLUSTER_LOCK_OP_REQUEST;
	req.current_mode = NoLock;
	req.lockmethod_id = DEFAULT_LOCKMETHOD;
	req.dontwait = true; /* conflict -> immediate 53RA8 (NOWAIT) */
	req.sessionLock = false;
	req.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	req.timeout_ms = cluster_ges_request_timeout_ms;
	/* A blocked TT waiter reuses the GES request wait (per spec-5.7 Q10 -- TT/DL/IR
	 * reuse ClusterGesReplyWait; only HW/KO get dedicated events).  NOWAIT here, so
	 * the wait event is not actually entered, but kept for request-shape parity. */
	req.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;

	r = cluster_lock_acquire_seven_step(&req);

	switch (r) {
	case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:
		/* Cluster/LMS layer inactive: the local pg_tablespace + TablespaceCreateLock
		 * locks fully cover a single-node DDL.  Proceed without the cross-node lock. */
		return TS_ACQUIRE_NATIVE;

	case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
		/* S3 reservation succeeded; TT has no PG-native heavyweight lock to take in
		 * between (mirror DL), so promote now to register the GRD holder.  A promote
		 * failure cannot guarantee the cross-node mutex -> fail closed. */
		if (cluster_lock_acquire_s5_promote(&req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return TS_ACQUIRE_FAILED;
		lk->held = true;
		lk->coordinated = true;
		lk->resid = *resid;
		lk->mode = mode;
		lk->req = req;
		return TS_ACQUIRE_GRANTED;

	case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
	case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:
		/* Legacy / stub S4 path: seven_step already ran S5 and returned an
		 * already-promoted holder.  Do NOT promote again -- just record it. */
		lk->held = true;
		lk->coordinated = true;
		lk->resid = *resid;
		lk->mode = mode;
		lk->req = req;
		return TS_ACQUIRE_GRANTED;

	default:
		/* NOT_AVAIL (a conflicting cross-node DDL holds the resid) / timeout /
		 * LMS-unavailable / internal: the cross-node mutex cannot be granted. */
		return TS_ACQUIRE_FAILED;
	}
}

static void
ts_unlock(ClusterTsLock *lk)
{
	if (lk == NULL || !lk->held)
		return;
	if (lk->coordinated)
		(void)cluster_lock_acquire_s6_release(&lk->req);
	lk->held = false;
	lk->coordinated = false;
}

/*
 * ts_lock_reset_cb -- TopTransactionContext reset callback: release the TT lock
 * when the top transaction that took it ends (commit OR abort).  A peer's
 * conflicting DDL is held off until then -- i.e. until this node's DDL is
 * durable.  Best-effort: never throws (it runs during commit/abort cleanup).
 */
static void
ts_lock_reset_cb(void *arg)
{
	ts_unlock((ClusterTsLock *)arg);
}

/*
 * ts_acquire_held_to_xact_end -- shared wrapper: gate, acquire TT(`mode`) on
 * `resid`, and on a coordinated grant arm the top-xact-end release.  `what` /
 * `name` feed the 53RA8 message.  No-op (returns) when coordination is off.
 */
static void
ts_acquire_held_to_xact_end(const ClusterResId *resid, LOCKMODE mode, const char *what,
							const char *name)
{
	ClusterTsLock *lk;
	TsAcquireOutcome out;

	/* Coordination only matters in a live, multi-node cluster with TT enabled. */
	if (!cluster_tablespace_ddl_lock_enabled || cluster_node_id < 0
		|| cluster_conf_node_count() <= 1 || RecoveryInProgress())
		return;

	/* The lock handle lives in TopTransactionContext so its reset callback fires
	 * when the top transaction this lock was taken in ends.  A DDL always runs in
	 * a transaction. */
	Assert(IsTransactionState());
	lk = (ClusterTsLock *)MemoryContextAllocZero(TopTransactionContext, sizeof(ClusterTsLock));
	out = ts_lock(resid, mode, lk);

	if (out == TS_ACQUIRE_FAILED) {
		pfree(lk);
		TS_BUMP(failclosed_count);
		ereport(ERROR,
				(errcode(ERRCODE_CLUSTER_TABLESPACE_LOCK_CONFLICT),
				 errmsg("could not acquire the cluster tablespace lock for %s \"%s\"", what, name),
				 errhint("Another node is running conflicting tablespace DDL, or the cluster "
						 "coordination layer is unavailable; retry.")));
	}

	if (out == TS_ACQUIRE_NATIVE) {
		/* Uncoordinated proceed: nothing held, reclaimed with TopTransactionContext
		 * at transaction end. */
		pfree(lk);
		TS_BUMP(native_count);
		return;
	}

	/* GRANTED: arm the top-xact-end release. */
	lk->reset_cb.func = ts_lock_reset_cb;
	lk->reset_cb.arg = lk;
	MemoryContextRegisterResetCallback(TopTransactionContext, &lk->reset_cb);
	if (mode == ShareLock)
		TS_BUMP(s_count);
	else
		TS_BUMP(x_count);
}


/* ============================================================
 * Public hooks (the tablespace.c / placement-DDL call sites).
 * ============================================================ */

void
cluster_ts_ddl_lock_x_oid(Oid tablespace_oid)
{
	ClusterResId resid;

	cluster_ts_resid_encode_oid(tablespace_oid, &resid);
	ts_acquire_held_to_xact_end(&resid, ExclusiveLock, "tablespace OID",
								psprintf("%u", tablespace_oid));
}

void
cluster_ts_ddl_lock_x_name(const char *spcname)
{
	ClusterResId resid;

	if (spcname == NULL)
		return;
	cluster_ts_resid_encode_name(spcname, &resid);
	ts_acquire_held_to_xact_end(&resid, ExclusiveLock, "tablespace", spcname);
}

void
cluster_ts_placement_lock_s(Oid tablespace_oid)
{
	ClusterResId resid;

	cluster_ts_resid_encode_oid(tablespace_oid, &resid);
	ts_acquire_held_to_xact_end(&resid, ShareLock, "placement into tablespace OID",
								psprintf("%u", tablespace_oid));
}


/* ============================================================
 * TEST-ONLY probe primitives (cluster_ts_srf.c).
 * ============================================================ */

int
cluster_ts_test_acquire(const ClusterResId *resid, LOCKMODE mode, ClusterTsLock *lk)
{
	TsAcquireOutcome out;

	out = ts_lock(resid, mode, lk);
	switch (out) {
	case TS_ACQUIRE_GRANTED:
		if (mode == ShareLock)
			TS_BUMP(s_count);
		else
			TS_BUMP(x_count);
		return 0;
	case TS_ACQUIRE_NATIVE:
		TS_BUMP(native_count);
		return 1;
	default:
		TS_BUMP(failclosed_count);
		return 2;
	}
}

void
cluster_ts_test_release(ClusterTsLock *lk)
{
	ts_unlock(lk);
}
