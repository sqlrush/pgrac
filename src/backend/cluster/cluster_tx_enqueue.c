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

#include "access/xact.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "portability/instr_time.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "cluster/cluster_cancel_token.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_lmd.h"
#include "cluster/cluster_lmd_wait_state.h"
#include "cluster/cluster_multixact_current_stats.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_status.h"
#include "cluster/cluster_tx_enqueue.h"

#ifdef USE_PGRAC_CLUSTER

/*
 * A waiter slot is owned by exactly one backend (indexed by pgprocno).  The
 * SOURCE owns the real TT key.  TARGET uses an opaque 24-byte, align-4 carrier
 * and converts only through memcpy to an aligned local ClusterTxLocator.  A
 * literal locator union would raise the slot alignment to 8 and grow every
 * existing 28-byte slot, which is forbidden by the R4 D9 shmem ABI lock.
 */
typedef union ClusterTxwIdentity {
	ClusterTTStatusKey source_key;
	uint32 target_locator_words[6];
} ClusterTxwIdentity;

typedef struct ClusterTxwWaitSlot {
	ClusterTxwIdentity identity;
	uint32 waiting; /* 0=FREE, 1=SOURCE, 2=TARGET, 3=current-MX SOURCE */
} ClusterTxwWaitSlot;

StaticAssertDecl(sizeof(ClusterTxwIdentity) == 24,
				 "R4 D9 waiter identity must remain exactly 24 bytes");
StaticAssertDecl(__alignof__(ClusterTxwIdentity) == 4, "R4 D9 waiter identity must remain align-4");
StaticAssertDecl(sizeof(ClusterTxwWaitSlot) == 28,
				 "R4 D9 waiter slot must remain exactly 28 bytes");
StaticAssertDecl(__alignof__(ClusterTxwWaitSlot) == 4, "R4 D9 waiter slot must remain align-4");

#define CLUSTER_TXW_SLOT_FREE 0
#define CLUSTER_TXW_SLOT_SOURCE 1
#define CLUSTER_TXW_SLOT_TARGET 2
#define CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX 3

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

static uint64
txw_monotonic_us(void)
{
	instr_time now;

	INSTR_TIME_SET_CURRENT(now);
	return (uint64)INSTR_TIME_GET_MICROSEC(now);
}

static uint64
txw_saturating_add_us(uint64 base, uint64 delta)
{
	return base > UINT64_MAX - delta ? UINT64_MAX : base + delta;
}


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
			memset(&ClusterTxw->slots[i].identity, 0, sizeof(ClusterTxwIdentity));
			ClusterTxw->slots[i].waiting = CLUSTER_TXW_SLOT_FREE;
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

static bool
txw_slot_set(int procno, const ClusterTTStatusKey *holder_key, bool current_mx_wait)
{
	bool registered = false;
	uint32 slot_kind
		= current_mx_wait ? CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX : CLUSTER_TXW_SLOT_SOURCE;

	LWLockAcquire(&ClusterTxw->lock, LW_EXCLUSIVE);
	if (ClusterTxw->slots[procno].waiting == CLUSTER_TXW_SLOT_FREE) {
		ClusterTxw->slots[procno].identity.source_key = *holder_key;
		ClusterTxw->slots[procno].waiting = slot_kind;
		pg_atomic_fetch_add_u32(&ClusterTxw->active_waiters, 1);
		registered = true;
	}
	LWLockRelease(&ClusterTxw->lock);
	return registered;
}

static bool
txw_target_slot_set_if_free(int procno, const ClusterTxLocator *aligned_locator)
{
	bool registered = false;

	LWLockAcquire(&ClusterTxw->lock, LW_EXCLUSIVE);
	if (ClusterTxw->slots[procno].waiting == CLUSTER_TXW_SLOT_FREE) {
		memcpy(ClusterTxw->slots[procno].identity.target_locator_words, aligned_locator,
			   sizeof(*aligned_locator));
		ClusterTxw->slots[procno].waiting = CLUSTER_TXW_SLOT_TARGET;
		pg_atomic_fetch_add_u32(&ClusterTxw->active_waiters, 1);
		registered = true;
	}
	LWLockRelease(&ClusterTxw->lock);
	return registered;
}

static void
txw_slot_clear(int procno, uint32 expected_kind)
{
	uint32 active;
	uint32 current_kind;

	Assert(expected_kind == CLUSTER_TXW_SLOT_SOURCE
		   || expected_kind == CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX
		   || expected_kind == CLUSTER_TXW_SLOT_TARGET);
	LWLockAcquire(&ClusterTxw->lock, LW_EXCLUSIVE);
	current_kind = ClusterTxw->slots[procno].waiting;
	if (current_kind == CLUSTER_TXW_SLOT_FREE) {
		LWLockRelease(&ClusterTxw->lock);
		return;
	}
	if (current_kind != expected_kind) {
		LWLockRelease(&ClusterTxw->lock);
		ereport(PANIC, (errmsg("cluster TX waiter slot ownership changed during cleanup")));
	}
	active = pg_atomic_read_u32(&ClusterTxw->active_waiters);
	if (active == 0) {
		LWLockRelease(&ClusterTxw->lock);
		ereport(PANIC, (errmsg("cluster TX active waiter counter underflow during cleanup")));
	}
	ClusterTxw->slots[procno].waiting = CLUSTER_TXW_SLOT_FREE;
	pg_atomic_fetch_sub_u32(&ClusterTxw->active_waiters, 1);
	LWLockRelease(&ClusterTxw->lock);
}

static uint32
txw_slot_kind(int procno)
{
	uint32 kind;

	LWLockAcquire(&ClusterTxw->lock, LW_SHARED);
	kind = ClusterTxw->slots[procno].waiting;
	LWLockRelease(&ClusterTxw->lock);
	return kind;
}

static void
txw_exit_slot_prevalidate(int procno, uint32 expected_kind)
{
	uint32 active;
	uint32 current_kind;

	Assert(expected_kind == CLUSTER_TXW_SLOT_SOURCE
		   || expected_kind == CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX
		   || expected_kind == CLUSTER_TXW_SLOT_TARGET);
	LWLockAcquire(&ClusterTxw->lock, LW_EXCLUSIVE);
	current_kind = ClusterTxw->slots[procno].waiting;
	if (current_kind != expected_kind) {
		LWLockRelease(&ClusterTxw->lock);
		ereport(PANIC,
				(errmsg("cluster TX waiter slot ownership changed during backend-exit cleanup")));
	}
	active = pg_atomic_read_u32(&ClusterTxw->active_waiters);
	if (active == 0) {
		LWLockRelease(&ClusterTxw->lock);
		ereport(PANIC,
				(errmsg("cluster TX active waiter counter underflow during backend-exit cleanup")));
	}
	LWLockRelease(&ClusterTxw->lock);
}

static void
txw_exact_wfg_cancel(const ClusterLmdVertex *waiter)
{
	ClusterLmdGraphRemoveResult remove_result;

	remove_result = cluster_lmd_graph_remove_edge_by_waiter_exact_result(waiter);
	if (remove_result != CLUSTER_LMD_GRAPH_REMOVE_REMOVED
		&& remove_result != CLUSTER_LMD_GRAPH_REMOVE_ABSENT)
		ereport(PANIC, (errmsg("cluster TX exact waiter identity became stale during cleanup")));
}

static void
txw_exact_waiter_vertex(ClusterLmdVertex *waiter, int procno, uint64 formation_epoch,
						TransactionId xid, uint64 wait_seq)
{
	memset(waiter, 0, sizeof(*waiter));
	waiter->node_id = cluster_node_id;
	waiter->procno = (uint32)procno;
	waiter->cluster_epoch = formation_epoch;
	waiter->request_id = 0;
	waiter->xid = xid;
	waiter->wait_seq = wait_seq;
}

static void
txw_exact_cleanup(int procno, uint32 slot_kind, bool slot_registered, bool wait_state_published,
				  bool wfg_registered, uint64 formation_epoch, TransactionId xid, uint64 wait_seq)
{
	if (wfg_registered) {
		ClusterLmdVertex waiter;

		txw_exact_waiter_vertex(&waiter, procno, formation_epoch, xid, wait_seq);
		txw_exact_wfg_cancel(&waiter);
	}
	if (wait_state_published)
		cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
	if (slot_registered)
		txw_slot_clear(procno, slot_kind);
}

/*
 * Stack cleanup closes normal and ERROR exits from both wait paths, but
 * proc_exit/FATAL abandons that C stack.  InitPostgres registers this
 * callback while shared memory and MyProc are still available.  SOURCE and
 * TARGET are the exact ownership discriminators; FREE remains a no-op.
 *
 * The owning backend cannot publish another wait while its exit callbacks
 * run, and PGPROC reuse starts only after they finish.  Therefore the stable
 * wait-state snapshot belongs to this SOURCE or TARGET slot.  Cancellation is
 * safe even when exit happened between wait-state publication and WFG edge
 * insertion because cancellation of an absent exact edge is idempotent.
 */
void
cluster_tx_enqueue_cleanup_on_backend_exit_callback(int code pg_attribute_unused(),
													Datum arg pg_attribute_unused())
{
	ClusterLmdWaitStateSnapshot wait_state;
	ClusterLmdWaitStateReadResult read_result;
	uint32 slot_kind;
	int procno;

	if (ClusterTxw == NULL || MyProc == NULL)
		return;

	procno = MyProc->pgprocno;
	if (procno < 0 || procno >= ClusterTxw->nslots)
		return;
	slot_kind = txw_slot_kind(procno);
	if (slot_kind == CLUSTER_TXW_SLOT_FREE)
		return;
	if (slot_kind != CLUSTER_TXW_SLOT_SOURCE && slot_kind != CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX
		&& slot_kind != CLUSTER_TXW_SLOT_TARGET)
		ereport(PANIC, (errmsg("cluster TX waiter slot has invalid ownership discriminator during "
							   "backend-exit cleanup")));

	memset(&wait_state, 0, sizeof(wait_state));
	read_result = cluster_lmd_wait_state_read_exact(&MyProc->cluster_lmd_wait, &wait_state);
	if (read_result == CLUSTER_LMD_WAIT_STATE_READ_BUSY)
		ereport(PANIC,
				(errmsg("cluster TX waiter state remained busy during backend-exit cleanup")));
	if (read_result == CLUSTER_LMD_WAIT_STATE_READ_ACTIVE) {
		if (!wait_state.active || wait_state.kind != CLUSTER_LMD_WAIT_TX
			|| wait_state.request_id != 0 || wait_state.wait_seq == 0)
			ereport(PANIC,
					(errmsg("cluster TX waiter state is malformed during backend-exit cleanup")));
	} else if (read_result != CLUSTER_LMD_WAIT_STATE_READ_INACTIVE)
		ereport(
			PANIC,
			(errmsg(
				"cluster TX waiter state has unknown read result during backend-exit cleanup")));
	txw_exit_slot_prevalidate(procno, slot_kind);

	/* ACTIVE cleanup order is exact WFG edge, proc wait state, owned slot. */
	if (read_result == CLUSTER_LMD_WAIT_STATE_READ_ACTIVE) {
		ClusterLmdVertex waiter;

		txw_exact_waiter_vertex(&waiter, procno, wait_state.cluster_epoch, wait_state.xid,
								wait_state.wait_seq);
		txw_exact_wfg_cancel(&waiter);
		cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
	}
	txw_slot_clear(procno, slot_kind);
}

static ClusterTxwResult
cluster_tx_enqueue_wait_internal(const ClusterTTStatusKey *holder_key, int effective_timeout_ms,
								 bool current_mx_wait, uint64 current_mx_deadline_mono_us)
{
	int procno;
	TimestampTz source_deadline = 0;
	ClusterTxwResult result = CLUSTER_TXW_TIMEOUT;
	ClusterLmdVertex tx_wfg_waiter;
	bool tx_wfg_registered = false;
	uint32 slot_kind
		= current_mx_wait ? CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX : CLUSTER_TXW_SLOT_SOURCE;

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

	if (current_mx_wait) {
		if (current_mx_deadline_mono_us == 0)
			return CLUSTER_TXW_UNPROVABLE;
	} else
		source_deadline = GetCurrentTimestamp() + (TimestampTz)effective_timeout_ms * 1000;

	if (!txw_slot_set(procno, holder_key, current_mx_wait))
		return CLUSTER_TXW_UNPROVABLE;
	pg_atomic_fetch_add_u64(&ClusterTxw->wait_count, 1);

	/*
	 * spec-5.8 D1d — publish the cluster wait-state so the LMD deadlock
	 * resolver can revalidate this backend is still genuinely waiting before
	 * it cancels it as a victim.  A TX wait carries no GES request id; the
	 * monotonic wait_seq plus procno + waiter xid identify the wait.
	 *
	 * spec-5.8 D2 (T1) — register a wait-for edge for the global deadlock
	 * detector.  The holder is known only by its transaction (origin node +
	 * xid from the TT status key); its backend procno is not carried, so the
	 * blocker is a placeholder (CLUSTER_LMD_TX_HOLDER_PROCNO) that the
	 * coordinator resolves by (node, xid) against the holder's own waiter
		 * vertex (T2).  A full edge table fails admission closed: waiting without
		 * the exact graph edge would make deadlock detection incomplete.
	 */
	{
		uint64 wait_epoch = cluster_epoch_get_current();
		TransactionId my_xid = GetTopTransactionIdIfAny();
		uint64 wait_seq;
		ClusterLmdVertex blocker;

		wait_seq = cluster_lmd_wait_state_publish(&MyProc->cluster_lmd_wait, CLUSTER_LMD_WAIT_TX, 0,
												  wait_epoch, my_xid);

		memset(&tx_wfg_waiter, 0, sizeof(tx_wfg_waiter));
		tx_wfg_waiter.node_id = cluster_node_id;
		tx_wfg_waiter.procno = (uint32)procno;
		tx_wfg_waiter.cluster_epoch = wait_epoch;
		tx_wfg_waiter.request_id = 0;
		tx_wfg_waiter.xid = my_xid;
		tx_wfg_waiter.wait_seq = wait_seq;

		memset(&blocker, 0, sizeof(blocker));
		blocker.node_id = holder_key->origin_node_id;
		blocker.procno = CLUSTER_LMD_TX_HOLDER_PROCNO;
		blocker.cluster_epoch = holder_key->cluster_epoch;
		blocker.request_id = 0;
		blocker.xid = holder_key->local_xid;

		tx_wfg_registered = cluster_lmd_submit_wait_edge_real(&tx_wfg_waiter, &blocker, 0);
	}
	if (!tx_wfg_registered) {
		cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
		txw_slot_clear(procno, slot_kind);
		return CLUSTER_TXW_UNPROVABLE;
	}

	/*
	 * The loop calls CHECK_FOR_INTERRUPTS() (query cancel / SIGTERM) and other
	 * code that can longjmp; wrap it so the waiter slot is ALWAYS released.
	 * Otherwise `waiting` / active_waiters leak and a later wake scan matches a
	 * stale slot (spurious-wake-safe, but a real resource leak).
	 */
	PG_TRY();
	{
		for (;;) {
			long wait_ms;

			ResetLatch(MyLatch);

			/*
				 * Consume only a token that still matches the live TX wait
				 * published above.  Do not ereport or return from inside
				 * PG_TRY: the shared cleanup envelope below must clear the
				 * waiter slot, wait-state, and WFG edge first.
				 */
			if (cluster_cancel_token_consume()) {
				result = CLUSTER_TXW_DEADLOCK;
				break;
			}

			/*
				 * Active R4 current-MX has no SOURCE authority to consult.  Keep
				 * the exact TX/WFG registration for one bounded native wake/poll
				 * slice, clean it below, and require the heap caller to restart
				 * from tuple capture + DESCRIBE.  The operation-owned absolute
				 * deadline survives that restart and prevents budget refresh.
				 */
			if (current_mx_wait) {
				uint64 now_us = txw_monotonic_us();
				uint64 remaining_us;

				if (now_us >= current_mx_deadline_mono_us) {
					result = CLUSTER_TXW_TIMEOUT;
					pg_atomic_fetch_add_u64(&ClusterTxw->timeout_count, 1);
					break;
				}
				remaining_us = current_mx_deadline_mono_us - now_us;
				if (remaining_us >= (uint64)CLUSTER_TXW_TICK_MS * UINT64_C(1000))
					wait_ms = CLUSTER_TXW_TICK_MS;
				else {
					wait_ms = (long)(remaining_us / UINT64_C(1000));
					if (remaining_us % UINT64_C(1000) != 0)
						wait_ms++;
					if (wait_ms <= 0)
						wait_ms = 1;
				}
				(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
								WAIT_EVENT_GES_TX_ENQUEUE_WAIT);
				CHECK_FOR_INTERRUPTS();
				result = CLUSTER_TXW_RETRY;
				break;
			}

			/* Re-check the holder's TT status (closes the register/wake race:
				 * a terminal status published before we slept is seen here). */
			{
				ClusterTTStatusResult cres;
				ClusterTTStatusSourceRequest source_request;
				ClusterTTStatusSourceResult source_result;
				bool found;
				TimestampTz now;

				memset(&source_request, 0, sizeof(source_request));
				source_request.key = holder_key;
				found = cluster_tt_status_source_dispatch(CLUSTER_TT_SOURCE_LOOKUP, &source_request,
														  &source_result)
							== CLUSTER_SEMANTIC_ADMISSION_OK
						&& source_result.bool_value;
				cres = source_result.lookup;
				if (found && cres.authoritative && txw_status_is_terminal(cres.status)) {
					result = CLUSTER_TXW_RESOLVED;
					break;
				}

				now = GetCurrentTimestamp();
				if (now >= source_deadline) {
					result = CLUSTER_TXW_TIMEOUT;
					pg_atomic_fetch_add_u64(&ClusterTxw->timeout_count, 1);
					break;
				}

				wait_ms = (long)((source_deadline - now) / 1000);
			}
			if (wait_ms <= 0)
				wait_ms = 1;
			if (wait_ms > CLUSTER_TXW_TICK_MS)
				wait_ms = CLUSTER_TXW_TICK_MS;

			(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
							WAIT_EVENT_GES_TX_ENQUEUE_WAIT);
			CHECK_FOR_INTERRUPTS();
		}
	}
	PG_CATCH();
	{
		if (tx_wfg_registered)
			txw_exact_wfg_cancel(&tx_wfg_waiter);
		cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
		txw_slot_clear(procno, slot_kind);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (tx_wfg_registered)
		txw_exact_wfg_cancel(&tx_wfg_waiter);
	cluster_lmd_wait_state_clear(&MyProc->cluster_lmd_wait);
	txw_slot_clear(procno, slot_kind);
	return result;
}

ClusterTxwResult
cluster_tx_enqueue_wait_exact(const ClusterTxLocator *locator, int effective_timeout_ms,
							  ClusterTxResolveReason *reason_out)
{
	ClusterTxResolution resolution;
	ClusterTxResolveReason initial_reason = CLUSTER_TX_RESOLVE_TARGET_DISABLED;
	ClusterTxOutcome initial_outcome;
	ClusterTxLocator target_locator;
	NodeId blocker_node;
	TransactionId my_xid;
	instr_time wait_started;
	uint64 formation_epoch;
	int procno;
	volatile bool slot_registered = false;
	volatile bool wait_state_published = false;
	volatile bool wfg_registered = false;
	volatile uint64 wait_seq = 0;
	volatile ClusterTxwResult result = CLUSTER_TXW_UNPROVABLE;
	volatile ClusterTxResolveReason final_reason = CLUSTER_TX_RESOLVE_PROTOCOL;

	Assert(AmRegularBackendProcess());
	if (reason_out != NULL)
		*reason_out = CLUSTER_TX_RESOLVE_TARGET_DISABLED;

	/* Admission/fixed-false ordering belongs to the exact resolver.  It must
	 * run before this layer inspects a possibly malformed locator or shmem. */
	memset(&resolution, 0, sizeof(resolution));
	initial_outcome = cluster_tx_resolve_exact(locator, CLUSTER_TX_RESOLVE_ROW_WAIT, &resolution,
											   &initial_reason);
	if (initial_outcome == CLUSTER_TX_UNKNOWN) {
		final_reason = initial_reason == CLUSTER_TX_RESOLVE_NONE ? CLUSTER_TX_RESOLVE_PROTOCOL
																 : initial_reason;
		goto done;
	}
	/* The exact resolver publishes a non-UNKNOWN outcome only with NONE. */
	Assert(initial_reason == CLUSTER_TX_RESOLVE_NONE);
	if (initial_outcome == CLUSTER_TX_COMMITTED || initial_outcome == CLUSTER_TX_ABORTED) {
		result = CLUSTER_TXW_RESOLVED;
		final_reason = CLUSTER_TX_RESOLVE_NONE;
		goto done;
	}
	if (initial_outcome != CLUSTER_TX_IN_PROGRESS && initial_outcome != CLUSTER_TX_PREPARED) {
		final_reason = CLUSTER_TX_RESOLVE_PROTOCOL;
		goto done;
	}

	if (ClusterTxw == NULL || MyProc == NULL) {
		final_reason = CLUSTER_TX_RESOLVE_CAPACITY;
		goto done;
	}
	procno = MyProc->pgprocno;
	if (procno < 0 || procno >= ClusterTxw->nslots) {
		final_reason = CLUSTER_TX_RESOLVE_CAPACITY;
		goto done;
	}

	/* The first resolver proved locator well-formed before a live/prepared
	 * result.  Copy into aligned process-local storage before using the
	 * 24-byte opaque shmem carrier. */
	memcpy(&target_locator, locator, sizeof(target_locator));
	blocker_node = uba_origin_node_id(target_locator.uba);
	if (!SCN_NODE_ID_VALID(blocker_node)) {
		final_reason = CLUSTER_TX_RESOLVE_BAD_UBA;
		goto done;
	}

	if (effective_timeout_ms <= 0)
		effective_timeout_ms = CLUSTER_TXW_DEFAULT_TIMEOUT_MS;
	formation_epoch = cluster_epoch_get_current();
	my_xid = GetTopTransactionIdIfAny();
	INSTR_TIME_SET_CURRENT(wait_started);

	PG_TRY();
	{
		do {
			ClusterLmdVertex waiter;
			ClusterLmdVertex blocker;

			if (cluster_epoch_get_current() != formation_epoch) {
				final_reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
				break;
			}
			if (!txw_target_slot_set_if_free(procno, &target_locator)) {
				final_reason = CLUSTER_TX_RESOLVE_REENTRANT;
				break;
			}
			slot_registered = true;
			pg_atomic_fetch_add_u64(&ClusterTxw->wait_count, 1);

			if (cluster_epoch_get_current() != formation_epoch) {
				final_reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
				break;
			}
			wait_seq = cluster_lmd_wait_state_publish(
				&MyProc->cluster_lmd_wait, CLUSTER_LMD_WAIT_TX, 0, formation_epoch, my_xid);
			wait_state_published = true;
			if (cluster_epoch_get_current() != formation_epoch) {
				final_reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
				break;
			}

			txw_exact_waiter_vertex(&waiter, procno, formation_epoch, my_xid, (uint64)wait_seq);
			memset(&blocker, 0, sizeof(blocker));
			blocker.node_id = blocker_node;
			blocker.procno = CLUSTER_LMD_TX_HOLDER_PROCNO;
			blocker.cluster_epoch = formation_epoch;
			blocker.request_id = 0;
			blocker.xid = target_locator.xid;
			if (!cluster_lmd_submit_wait_edge_real(&waiter, &blocker, 0)) {
				final_reason = CLUSTER_TX_RESOLVE_CAPACITY;
				break;
			}
			wfg_registered = true;
			if (cluster_epoch_get_current() != formation_epoch) {
				final_reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
				break;
			}

			for (;;) {
				ClusterTxResolveReason current_reason = CLUSTER_TX_RESOLVE_PROTOCOL;
				ClusterTxOutcome current_outcome;
				instr_time elapsed;
				instr_time now;
				double elapsed_ms;
				long wait_ms;

				ResetLatch(MyLatch);
				if (cluster_epoch_get_current() != formation_epoch) {
					final_reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
					break;
				}
				memset(&resolution, 0, sizeof(resolution));
				current_outcome = cluster_tx_resolve_exact(
					&target_locator, CLUSTER_TX_RESOLVE_ROW_WAIT, &resolution, &current_reason);
				if (cluster_epoch_get_current() != formation_epoch) {
					final_reason = CLUSTER_TX_RESOLVE_RF_DEFERRED;
					break;
				}

				if (current_outcome == CLUSTER_TX_COMMITTED
					|| current_outcome == CLUSTER_TX_ABORTED) {
					result = CLUSTER_TXW_RESOLVED;
					final_reason = CLUSTER_TX_RESOLVE_NONE;
					break;
				}
				if (current_outcome == CLUSTER_TX_UNKNOWN) {
					final_reason = current_reason == CLUSTER_TX_RESOLVE_NONE
									   ? CLUSTER_TX_RESOLVE_PROTOCOL
									   : current_reason;
					break;
				}
				if ((current_outcome != CLUSTER_TX_IN_PROGRESS
					 && current_outcome != CLUSTER_TX_PREPARED)
					|| current_reason != CLUSTER_TX_RESOLVE_NONE) {
					final_reason = CLUSTER_TX_RESOLVE_PROTOCOL;
					break;
				}

				INSTR_TIME_SET_CURRENT(now);
				elapsed = now;
				INSTR_TIME_SUBTRACT(elapsed, wait_started);
				elapsed_ms = INSTR_TIME_GET_MILLISEC(elapsed);
				if (elapsed_ms >= (double)effective_timeout_ms) {
					result = CLUSTER_TXW_TIMEOUT;
					final_reason = CLUSTER_TX_RESOLVE_TIMEOUT;
					pg_atomic_fetch_add_u64(&ClusterTxw->timeout_count, 1);
					break;
				}

				wait_ms = (long)((double)effective_timeout_ms - elapsed_ms);
				if (wait_ms <= 0)
					wait_ms = 1;
				if (wait_ms > CLUSTER_TXW_TICK_MS)
					wait_ms = CLUSTER_TXW_TICK_MS;
				(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, wait_ms,
								WAIT_EVENT_GES_TX_ENQUEUE_WAIT);
				CHECK_FOR_INTERRUPTS();
			}
		} while (false);
	}
	PG_FINALLY();
	{
		txw_exact_cleanup(procno, CLUSTER_TXW_SLOT_TARGET, (bool)slot_registered,
						  (bool)wait_state_published, (bool)wfg_registered, formation_epoch, my_xid,
						  (uint64)wait_seq);
	}
	PG_END_TRY();

done:
	if (reason_out != NULL)
		*reason_out = (ClusterTxResolveReason)final_reason;
	return (ClusterTxwResult)result;
}

ClusterTxwResult
cluster_tx_enqueue_wait(const ClusterTTStatusKey *holder_key, int effective_timeout_ms)
{
	return cluster_tx_enqueue_wait_internal(holder_key, effective_timeout_ms, false, 0);
}

ClusterTxwResult
cluster_tx_enqueue_wait_current_mx(const ClusterTTStatusKey *holder_key, int effective_timeout_ms,
								   uint64 *absolute_deadline_mono_us)
{
	uint64 now_us;
	uint64 timeout_us;

	if (absolute_deadline_mono_us == NULL)
		return CLUSTER_TXW_UNPROVABLE;
	if (effective_timeout_ms <= 0)
		effective_timeout_ms = CLUSTER_TXW_DEFAULT_TIMEOUT_MS;
	if (*absolute_deadline_mono_us == 0) {
		now_us = txw_monotonic_us();
		timeout_us = (uint64)effective_timeout_ms > UINT64_MAX / UINT64_C(1000)
						 ? UINT64_MAX
						 : (uint64)effective_timeout_ms * UINT64_C(1000);
		*absolute_deadline_mono_us = txw_saturating_add_us(now_us, timeout_us);
	}
	return cluster_tx_enqueue_wait_internal(holder_key, effective_timeout_ms, true,
											*absolute_deadline_mono_us);
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
		if ((ClusterTxw->slots[i].waiting == CLUSTER_TXW_SLOT_SOURCE
			 || ClusterTxw->slots[i].waiting == CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX)
			&& txw_key_equal(&ClusterTxw->slots[i].identity.source_key, holder_key)) {
			pg_atomic_fetch_add_u64(&ClusterTxw->wakeup_count, 1);
			if (ClusterTxw->slots[i].waiting == CLUSTER_TXW_SLOT_SOURCE_CURRENT_MX)
				cluster_multixact_current_stats_bump(CMX_STAT_WAKEUP);
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
