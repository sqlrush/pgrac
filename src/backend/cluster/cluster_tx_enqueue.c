/*-------------------------------------------------------------------------
 *
 * cluster_tx_enqueue.c
 *	  pgrac cross-node TX enqueue completion wait (Oracle TX enqueue model).
 *
 *	  spec-5.2 D4/D6.  A backend that finds a tuple locked by a REMOTE
 *	  transaction registers a per-backend waiter slot keyed by the holder's
 *	  full ClusterTTStatusKey, then blocks on its latch until the holder's
 *	  TT status becomes terminal (the TT-status-hint receiver calls
 *	  cluster_txw_wake_waiters) or a finite timeout elapses.  The caller
 *	  then re-judges (re-reads xmax); this layer never returns a visibility
 *	  verdict and never touches a tuple.
 *
 *	  Correctness (Rule 8.A):
 *	    - exact 24B key match (H1):  slot reuse with the same raw xid must
 *	      not cross-wake;
 *	    - spurious wake safe:  the caller re-checks the holder TT status, so
 *	      an over-eager SetLatch only costs one extra TT lookup;
 *	    - missed wake bounded:  a finite timeout backstops a lost wake
 *	      (the wait never hangs forever).
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tx_enqueue.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tx_enqueue.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * A waiter slot is owned by exactly one backend (indexed by pgprocno).  The
 * owner writes holder_key first, then publishes by setting waiting = 1 with a
 * write barrier; the waker reads waiting with a read barrier before trusting
 * holder_key.  The slot LWLock serializes the scan against set/clear so the
 * key read is never torn.
 */
typedef struct ClusterTxwWaitSlot {
	ClusterTTStatusKey holder_key; /* full 24B exact identity (H1) */
	uint32 waiting;				   /* 1 = owner backend is blocked */
} ClusterTxwWaitSlot;

typedef struct ClusterTxwShmem {
	LWLock lock; /* protects the slot scan / set / clear */
	int nslots;
	pg_atomic_uint32 active_waiters; /* >0 => a wake scan may be worthwhile */
	pg_atomic_uint64 wait_count;
	pg_atomic_uint64 wakeup_count;
	pg_atomic_uint64 timeout_count;
	ClusterTxwWaitSlot slots[FLEXIBLE_ARRAY_MEMBER];
} ClusterTxwShmem;

static ClusterTxwShmem *ClusterTxw = NULL;

/* The default finite wait budget when the caller supplies a non-positive
 * timeout — perpetual (-1) is forbidden (Q6 / 5.1b clause 8). */
#define CLUSTER_TXW_DEFAULT_TIMEOUT_MS 60000

/* Single-tick cap so the timeout / interrupt checks stay responsive even if
 * a wake is lost. */
#define CLUSTER_TXW_TICK_MS 1000


static inline bool
txw_key_equal(const ClusterTTStatusKey *a, const ClusterTTStatusKey *b)
{
	/* H1:  every identity field must match — raw local_xid alone is unsafe. */
	return a->origin_node_id == b->origin_node_id && a->undo_segment_id == b->undo_segment_id
		   && a->tt_slot_id == b->tt_slot_id && a->cluster_epoch == b->cluster_epoch
		   && TransactionIdEquals(a->local_xid, b->local_xid);
}

static inline bool
txw_status_is_terminal(ClusterTTStatus status)
{
	return status == CLUSTER_TT_STATUS_COMMITTED || status == CLUSTER_TT_STATUS_ABORTED
		   || status == CLUSTER_TT_STATUS_CLEANED_OUT;
}


/* ============================================================
 * Shmem region.
 * ============================================================ */

Size
cluster_tx_enqueue_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return MAXALIGN(offsetof(ClusterTxwShmem, slots)
					+ (Size)MaxBackends * sizeof(ClusterTxwWaitSlot));
}

void
cluster_tx_enqueue_shmem_init(void)
{
	bool found;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	ClusterTxw = (ClusterTxwShmem *)ShmemInitStruct("pgrac cluster tx enqueue",
													cluster_tx_enqueue_shmem_size(), &found);
	if (!found) {
		int i;

		LWLockInitialize(&ClusterTxw->lock, LWTRANCHE_CLUSTER_TT_STATUS);
		ClusterTxw->nslots = MaxBackends;
		pg_atomic_init_u32(&ClusterTxw->active_waiters, 0);
		pg_atomic_init_u64(&ClusterTxw->wait_count, 0);
		pg_atomic_init_u64(&ClusterTxw->wakeup_count, 0);
		pg_atomic_init_u64(&ClusterTxw->timeout_count, 0);
		for (i = 0; i < MaxBackends; i++) {
			memset(&ClusterTxw->slots[i].holder_key, 0, sizeof(ClusterTTStatusKey));
			ClusterTxw->slots[i].waiting = 0;
		}
	}
}

static const ClusterShmemRegion cluster_tx_enqueue_region = {
	.name = "pgrac cluster tx enqueue",
	.size_fn = cluster_tx_enqueue_shmem_size,
	.init_fn = cluster_tx_enqueue_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_tx_enqueue",
	.reserved_flags = 0,
};

void
cluster_tx_enqueue_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_tx_enqueue_region);
}


/* ============================================================
 * Wait / wake.
 * ============================================================ */

static void
txw_slot_set(int procno, const ClusterTTStatusKey *holder_key)
{
	LWLockAcquire(&ClusterTxw->lock, LW_EXCLUSIVE);
	if (ClusterTxw->slots[procno].waiting == 0)
		pg_atomic_fetch_add_u32(&ClusterTxw->active_waiters, 1);
	ClusterTxw->slots[procno].holder_key = *holder_key;
	ClusterTxw->slots[procno].waiting = 1;
	LWLockRelease(&ClusterTxw->lock);
}

static void
txw_slot_clear(int procno)
{
	LWLockAcquire(&ClusterTxw->lock, LW_EXCLUSIVE);
	if (ClusterTxw->slots[procno].waiting != 0)
		pg_atomic_fetch_sub_u32(&ClusterTxw->active_waiters, 1);
	ClusterTxw->slots[procno].waiting = 0;
	LWLockRelease(&ClusterTxw->lock);
}

ClusterTxwResult
cluster_tx_enqueue_wait(const ClusterTTStatusKey *holder_key, int effective_timeout_ms)
{
	int procno;
	TimestampTz deadline;
	ClusterTxwResult result = CLUSTER_TXW_TIMEOUT;

	Assert(holder_key != NULL);

	/* Perpetual wait is forbidden — clamp to a finite budget (Q6). */
	if (effective_timeout_ms <= 0)
		effective_timeout_ms = CLUSTER_TXW_DEFAULT_TIMEOUT_MS;

	if (ClusterTxw == NULL || MyProc == NULL) {
		/* Not attached (single node / bootstrap) — the caller's branch
		 * gates on cluster_peer_mode_enabled(), so this is defensive. */
		return CLUSTER_TXW_TIMEOUT;
	}

	procno = MyProc->pgprocno;
	if (procno < 0 || procno >= ClusterTxw->nslots) {
		/* Auxiliary / out-of-range proc — cannot register a slot.  Fail the
		 * wait closed (the caller re-judges or errors), never a stale grant. */
		return CLUSTER_TXW_TIMEOUT;
	}

	pg_atomic_fetch_add_u64(&ClusterTxw->wait_count, 1);
	deadline = GetCurrentTimestamp() + (TimestampTz)effective_timeout_ms * 1000;

	txw_slot_set(procno, holder_key);

	for (;;) {
		ClusterTTStatusResult cres;
		bool found;
		TimestampTz now;
		long wait_ms;

		ResetLatch(MyLatch);

		/* Re-check the holder's TT status (closes the register/wake race:
		 * a terminal status published before we slept is seen here). */
		found = cluster_tt_status_lookup_exact(holder_key, &cres);
		if (found && cres.authoritative && txw_status_is_terminal(cres.status)) {
			result = CLUSTER_TXW_RESOLVED;
			break;
		}

		now = GetCurrentTimestamp();
		if (now >= deadline) {
			result = CLUSTER_TXW_TIMEOUT;
			pg_atomic_fetch_add_u64(&ClusterTxw->timeout_count, 1);
			break;
		}

		wait_ms = (long)((deadline - now) / 1000);
		if (wait_ms <= 0)
			wait_ms = 1;
		if (wait_ms > CLUSTER_TXW_TICK_MS)
			wait_ms = CLUSTER_TXW_TICK_MS;

		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
						WAIT_EVENT_GES_GRANT_WAIT);
		CHECK_FOR_INTERRUPTS();
	}

	txw_slot_clear(procno);
	return result;
}

void
cluster_txw_wake_waiters(const ClusterTTStatusKey *holder_key)
{
	int i;
	int nslots;

	if (ClusterTxw == NULL || holder_key == NULL)
		return;

	/* Fast path: this runs on every terminal TT install (commit/abort hot
	 * path) — skip the LWLock + scan entirely when nobody is waiting. */
	if (pg_atomic_read_u32(&ClusterTxw->active_waiters) == 0)
		return;

	LWLockAcquire(&ClusterTxw->lock, LW_SHARED);
	nslots = ClusterTxw->nslots;
	for (i = 0; i < nslots; i++) {
		if (ClusterTxw->slots[i].waiting != 0
			&& txw_key_equal(&ClusterTxw->slots[i].holder_key, holder_key)) {
			pg_atomic_fetch_add_u64(&ClusterTxw->wakeup_count, 1);
			SetLatch(&GetPGProcByNumber(i)->procLatch);
		}
	}
	LWLockRelease(&ClusterTxw->lock);
}


/* ============================================================
 * Counters.
 * ============================================================ */

uint64
cluster_txw_get_wait_count(void)
{
	return ClusterTxw ? pg_atomic_read_u64(&ClusterTxw->wait_count) : 0;
}

uint64
cluster_txw_get_wakeup_count(void)
{
	return ClusterTxw ? pg_atomic_read_u64(&ClusterTxw->wakeup_count) : 0;
}

uint64
cluster_txw_get_timeout_count(void)
{
	return ClusterTxw ? pg_atomic_read_u64(&ClusterTxw->timeout_count) : 0;
}

#endif /* USE_PGRAC_CLUSTER */
