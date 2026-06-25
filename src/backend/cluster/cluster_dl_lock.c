/*-------------------------------------------------------------------------
 *
 * cluster_dl_lock.c
 *	  DL (bulk-load lease) backend: shmem counters + GES acquire/release +
 *	  BulkInsertState lease wrappers (spec-5.7 §3.2 / D4).  The pure resid
 *	  encoder lives in cluster_dl.c (standalone-linkable for the unit test).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_dl_lock.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D4, §3.2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/hio.h"	 /* BulkInsertStateData (full struct) */
#include "access/xact.h" /* IsTransactionState, CurTransactionContext */
#include "access/xlog.h" /* RecoveryInProgress */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_dl.h"
#include "cluster/cluster_extend_gate.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_hw.h" /* cluster_hw_classify_persistence */
#include "cluster/cluster_lock_acquire.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/lock.h"
#include "storage/shmem.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"
#include "port/atomics.h"

/* ============================================================
 * Shmem region: three observability counters.
 * ============================================================ */

typedef struct ClusterDlShared {
	pg_atomic_uint64 lease_count;	   /* DL(X) leases granted (coordinated) */
	pg_atomic_uint64 native_count;	   /* uncoordinated / native proceed */
	pg_atomic_uint64 failclosed_count; /* 53RA7 fail-closed */
	pg_atomic_uint64 release_count;	   /* coordinated leases released */
} ClusterDlShared;

static ClusterDlShared *dl_state = NULL;

Size
cluster_dl_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterDlShared));
}

void
cluster_dl_shmem_init(void)
{
	bool found;

	dl_state = (ClusterDlShared *)ShmemInitStruct("pgrac cluster dl",
												  MAXALIGN(sizeof(ClusterDlShared)), &found);
	if (!IsUnderPostmaster) {
		pg_atomic_init_u64(&dl_state->lease_count, 0);
		pg_atomic_init_u64(&dl_state->native_count, 0);
		pg_atomic_init_u64(&dl_state->failclosed_count, 0);
		pg_atomic_init_u64(&dl_state->release_count, 0);
	}
}

static const ClusterShmemRegion cluster_dl_region = {
	.name = "pgrac cluster dl",
	.size_fn = cluster_dl_shmem_size,
	.init_fn = cluster_dl_shmem_init,
	.lwlock_count = 0,
	.owner_subsys = "spec-5.7 DL bulk-load lease",
	.reserved_flags = 0,
};

void
cluster_dl_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_dl_region);
}

#define DL_BUMP(field)                                                                             \
	do {                                                                                           \
		if (dl_state != NULL)                                                                      \
			pg_atomic_fetch_add_u64(&dl_state->field, 1);                                          \
	} while (0)

uint64
cluster_dl_lease_count(void)
{
	return dl_state != NULL ? pg_atomic_read_u64(&dl_state->lease_count) : 0;
}
uint64
cluster_dl_native_count(void)
{
	return dl_state != NULL ? pg_atomic_read_u64(&dl_state->native_count) : 0;
}
uint64
cluster_dl_failclosed_count(void)
{
	return dl_state != NULL ? pg_atomic_read_u64(&dl_state->failclosed_count) : 0;
}
uint64
cluster_dl_release_count(void)
{
	return dl_state != NULL ? pg_atomic_read_u64(&dl_state->release_count) : 0;
}


/* ============================================================
 * GES acquire / release over the spec-5.3 substrate.
 * ============================================================ */

/*
 * dl_lock -- acquire DL(X) on `resid` over the GES substrate (mirror
 * cluster_hw_lock).  Fills *lk; returns one of:
 *   GRANTED   coordinated lease held (lk->held = lk->coordinated = true).
 *   NATIVE    cluster/LMS layer inactive -- NOT coordinated, but the caller
 *             PROCEEDS (HW gates correctness at the extend).  lk->held = false.
 *   FAILED    a genuine grant failure (timeout / NOT_AVAIL / deadlock / S5) --
 *             the caller fails closed 53RA7.
 */
typedef enum DlAcquireOutcome {
	DL_ACQUIRE_GRANTED = 0,
	DL_ACQUIRE_NATIVE,
	DL_ACQUIRE_FAILED,
} DlAcquireOutcome;

static DlAcquireOutcome
dl_lock(const ClusterResId *resid, DlLock *lk)
{
	ClusterLockAcquireRequest req;
	ClusterLockAcquireResult r;

	Assert(resid != NULL && lk != NULL);
	memset(lk, 0, sizeof(*lk));

	memset(&req, 0, sizeof(req));
	req.resid = *resid;
	req.lockmode = ExclusiveLock;
	req.op = CLUSTER_LOCK_OP_REQUEST; /* DL never converts */
	req.current_mode = NoLock;
	req.lockmethod_id = DEFAULT_LOCKMETHOD;
	req.dontwait = false;
	req.sessionLock = false;
	req.caller_local_start_ts_ms = (uint64)(GetCurrentTimestamp() / 1000);
	req.timeout_ms = cluster_ges_request_timeout_ms;
	/*
	 * A blocked DL waiter is an "awaiting a GES grant" wait, so it reuses the GES
	 * reply wait event (shows as ClusterGesReplyWait) -- per spec-5.7 Q10, DL / TT
	 * / IR reuse the existing GES request wait, and only HW (ClusterRelExtendWait)
	 * and KO (ClusterObjectFlushWait) get dedicated events because their waits are
	 * a distinct kind (hot-path authority / peer-ACK).  This satisfies Rule 17
	 * (the blocking path owns a wait event) without a redundant DL-specific one.
	 */
	req.wait_event = WAIT_EVENT_CLUSTER_GES_REPLY_WAIT;

	r = cluster_lock_acquire_seven_step(&req);

	switch (r) {
	case CLUSTER_LOCK_ACQUIRE_OK_NATIVE:
		/*
		 * cluster/LMS layer inactive.  Unlike HW (correctness-critical, fails
		 * closed here), DL is a coordination lease that HW backs: proceed without
		 * the lease -- the extend's HW acquire is what fails closed if cross-node
		 * coordination is genuinely required.  Never block a bulk load on the
		 * coordination layer being warm.
		 */
		return DL_ACQUIRE_NATIVE;

	case CLUSTER_LOCK_ACQUIRE_NEED_PG_NATIVE_LOCK:
		/*
		 * S3 local-fast-path or modern S4 wire success: seven_step did NOT run S5
		 * (it leaves the reservation for the caller to promote -- DL has no
		 * PG-native heavyweight lock to acquire in between).  Promote now to
		 * register the GRD holder for cross-node conflict; S5 failure fails closed.
		 */
		if (cluster_lock_acquire_s5_promote(&req) != CLUSTER_LOCK_ACQUIRE_OK_GRANTED)
			return DL_ACQUIRE_FAILED;
		lk->held = true;
		lk->coordinated = true;
		lk->resid = *resid;
		lk->req = req;
		return DL_ACQUIRE_GRANTED;

	case CLUSTER_LOCK_ACQUIRE_OK_GRANTED:
	case CLUSTER_LOCK_ACQUIRE_OK_CONVERTED:
		/*
		 * Legacy / stub S4 path only: seven_step already ran S5 internally and
		 * returned an already-promoted holder.  Do NOT promote a second time
		 * (double-promote would re-revalidate an entry that is no longer a
		 * reservation).  The lease is held -- just record it.  The modern real
		 * wire returns NEED_PG_NATIVE_LOCK, so this arm is the legacy path; kept
		 * for forward-compat with the GES wire rather than failing closed (which
		 * would strand the holder seven_step already registered).
		 */
		lk->held = true;
		lk->coordinated = true;
		lk->resid = *resid;
		lk->req = req;
		return DL_ACQUIRE_GRANTED;

	default:
		/* NOT_AVAIL / timeout / LMS-unavailable / deadlock / internal. */
		return DL_ACQUIRE_FAILED;
	}
}

static void
dl_unlock(DlLock *lk)
{
	if (lk == NULL || !lk->held)
		return;
	if (lk->coordinated) {
		(void)cluster_lock_acquire_s6_release(&lk->req);
		DL_BUMP(release_count);
	}
	lk->held = false;
	lk->coordinated = false;
}

/*
 * dl_lease_reset_cb -- CurTransactionContext reset callback (the (sub)xact-end
 * backstop, see DlLock).  Releases the lease if it is still held -- i.e. the
 * bulk load did NOT reach cluster_dl_bulk_release (a COPY/CTAS that ERRORed
 * mid-load).  dl_unlock is idempotent, so when the explicit release already
 * ran this is a no-op.  Best-effort: never throws (it runs during abort).
 */
static void
dl_lease_reset_cb(void *arg)
{
	dl_unlock((DlLock *)arg);
}


/* ============================================================
 * BulkInsertState lease wrappers (the DL-M1 hook).
 * ============================================================ */

void
cluster_dl_bulk_acquire(struct RelationData *rel, struct BulkInsertStateData *bistate)
{
	ClusterResId resid;
	DlLock *lk;
	DlAcquireOutcome out;

	/* Regular insert (no bulk path) -> no DL; HW alone covers it (§3.2). */
	if (bistate == NULL || rel == NULL)
		return;
	/* DL already evaluated for this bulk load (a held lease OR a cached no-lease
	 * native decision) -- subsequent batches/rows skip the GES round trip. */
	if (bistate->cluster_dl != NULL)
		return;
	/* Same gate as the HW extend authority: coordination only matters in a live
	 * cluster with the relation-extend coordination enabled. */
	if (!cluster_relation_extend_lock_enabled || cluster_node_id < 0 || RecoveryInProgress())
		return;
	/* Only GLOBALIZE relations are cross-node extended; temp / unlogged are
	 * handled (or fail-closed) by HW at the extend, so DL stays out of their way. */
	if (cluster_hw_classify_persistence(rel->rd_rel->relpersistence, true) != CLUSTER_HW_GLOBALIZE)
		return;
	/*
	 * Engage from runtime liveness, not the static configured node count
	 * (spec-5.7 §3.1d).  wait_for_lms = false: DL must never block a bulk load on
	 * the coordination layer warming up -- dl_lock fails open to a native lease
	 * and HW still gates correctness at each extend.  When no peer is alive
	 * (NATIVE) there is nothing to coordinate, so skip the lease entirely.
	 */
	if (cluster_extend_liveness_engage(false) == CLUSTER_EXTEND_ENGAGE_NATIVE)
		return;

	cluster_dl_resid_encode(RelationGetSmgr(rel)->smgr_rlocator.locator, &resid);

	/*
	 * The lease handle lives in CurTransactionContext so its reset callback (the
	 * (sub)xact-end release backstop, registered below for a coordinated lease)
	 * fires when the (sub)transaction this lease was taken in ends -- covering a
	 * top-level abort AND a savepoint rollback mid-load.  A bulk load always runs
	 * inside a transaction.
	 */
	Assert(IsTransactionState());
	lk = (DlLock *)MemoryContextAllocZero(CurTransactionContext, sizeof(DlLock));
	out = dl_lock(&resid, lk);

	if (out == DL_ACQUIRE_FAILED) {
		pfree(lk);
		DL_BUMP(failclosed_count);
		ereport(ERROR, (errcode(ERRCODE_CLUSTER_BULK_LOAD_LEASE_UNAVAILABLE),
						errmsg("could not acquire the cluster bulk-load lease for \"%s\"",
							   RelationGetRelationName(rel)),
						errhint("Another node may be bulk-loading this relation, or the cluster "
								"coordination layer is unavailable; retry.")));
	}

	if (out == DL_ACQUIRE_NATIVE) {
		/*
		 * Uncoordinated proceed (the GES substrate resolved this resource to a
		 * native/local lock -- e.g. a new-in-txn private relation a CTAS / matview
		 * is building, which no peer can see and so needs no cross-node lease; HW
		 * still gates correctness at the extend).  CACHE the no-lease decision on
		 * the BulkInsertState (held = coordinated = false) so the remaining rows /
		 * batches of this bulk load skip the GES round trip rather than re-probe it
		 * per row.  No backstop callback (nothing is held); the DlLock is reclaimed
		 * with CurTransactionContext at (sub)transaction end.
		 */
		bistate->cluster_dl = lk; /* lk->held / lk->coordinated are already false */
		DL_BUMP(native_count);
		return;
	}

	/*
	 * GRANTED: retain the lease on the BulkInsertState for explicit release at
	 * FreeBulkInsertState, and arm the CurTransactionContext backstop so an
	 * aborted bulk load (FreeBulkInsertState skipped) still releases at (sub)xact
	 * end.
	 */
	lk->reset_cb.func = dl_lease_reset_cb;
	lk->reset_cb.arg = lk;
	MemoryContextRegisterResetCallback(CurTransactionContext, &lk->reset_cb);
	bistate->cluster_dl = lk;
	DL_BUMP(lease_count);
}

void
cluster_dl_bulk_release(struct BulkInsertStateData *bistate)
{
	DlLock *lk;

	if (bistate == NULL || bistate->cluster_dl == NULL)
		return;
	lk = (DlLock *)bistate->cluster_dl;
	dl_unlock(lk);
	/*
	 * Do NOT pfree lk here: for a coordinated lease its reset callback is still
	 * registered on CurTransactionContext (callbacks cannot be unregistered), so
	 * freeing now would leave a dangling callback.  The context reclaims lk (and
	 * fires the now-no-op callback) at (sub)transaction end.
	 */
	bistate->cluster_dl = NULL;
}
