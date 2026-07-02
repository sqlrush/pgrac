/*-------------------------------------------------------------------------
 *
 * cluster_ges_reply_wait.c
 *	  Cross-node GES reply wait table — spec-2.23 D1.
 *
 *	  Per-backend reply correlation HTAB keyed by HC17 5-tuple.  See
 *	  cluster_ges_reply_wait.h for protocol contract.
 *
 *	  Step 1 (D1) ships:  shmem region, HTAB allocation, insert / lookup
 *	  / wake / delete / sweep_timeout API bodies, counter accessors.
 *	  The CV-driven wait loop in cluster_ges_send_request_and_wait
 *	  (caller side) lands Step 2 D2.
 *
 *	  HC17 invariant (spec-2.23 §3.2):  every successful insert MUST be
 *	  paired with a delete (normal wake or timeout path); late reply
 *	  (lookup miss) is silently dropped + counter++.
 *
 *	  Spec: spec-2.23-cross-node-ges-bast-deadlock-production.md (FROZEN v0.3)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ges_reply_wait.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "cluster/cluster_ges.h" /* spec-5.16: GES_REPLY_OPCODE_GRANT (orphan tombstone) */
#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_xnode_profile.h" /* PGRAC: spec-5.59 D2 profiling */


/*
 * spec-2.23 D1 cap — hardcoded for Step 1; Step 9 D11 replaces with
 * GUC cluster.ges_reply_wait_max_entries (PGC_POSTMASTER, default 1024,
 * min 64 max 65536).  HTAB and shmem sizing both consume this value at
 * shmem_size / shmem_init time.
 */
#define CLUSTER_GES_REPLY_WAIT_DEFAULT_MAX 1024


/* ============================================================
 * Shmem state.
 * ============================================================ */

typedef struct ClusterGesReplyWaitShared {
	LWLock lwlock;							  /* guards HTAB structure */
	pg_atomic_uint64 reply_wait_table_active; /* live entry count */
	pg_atomic_uint64 reply_late_drop_count;	  /* HC17 late reply drops */
	pg_atomic_uint64 release_ack_count;		  /* spec-2.23 D3 wire */
	pg_atomic_uint64 sweep_deleted_count;	  /* timeout sweep total */
	/*
	 * spec-5.16 (Rule 8.A) — node-global request_id sequence.  The reply-wait HTAB
	 * key is the 5-tuple {request_id, source, dest, opcode, epoch}; for that key to
	 * be unique per in-flight request, request_id MUST be unique node-wide.  The
	 * lock-acquire path historically used a *backend-local* counter, so every fresh
	 * backend's first acquire reused request_id=1 — two such requests to the same
	 * master/epoch collided on one key, which was masked only because entries were
	 * short-lived (delete-on-timeout).  The orphan tombstone (long-lived) exposed
	 * the collision (a fresh same-key insert hits found==true -> spurious fail-close).
	 * Sourcing request_id from this node-global counter makes the key truly unique.
	 */
	pg_atomic_uint64 request_id_seq;
} ClusterGesReplyWaitShared;

static ClusterGesReplyWaitShared *reply_wait_state = NULL;
static HTAB *reply_wait_htab = NULL;


/* ============================================================
 * Forward declarations.
 * ============================================================ */

static const ClusterShmemRegion cluster_ges_reply_wait_region = {
	.name = "pgrac cluster ges reply wait",
	.size_fn = cluster_ges_reply_wait_shmem_size,
	.init_fn = cluster_ges_reply_wait_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "spec-2.23 GES reply wait",
	.reserved_flags = 0,
};


/* ============================================================
 * Shmem region request / init / register.
 * ============================================================ */

Size
cluster_ges_reply_wait_shmem_size(void)
{
	Size sz = MAXALIGN(sizeof(ClusterGesReplyWaitShared));

	sz = add_size(sz, hash_estimate_size((Size)CLUSTER_GES_REPLY_WAIT_DEFAULT_MAX,
										 sizeof(GesReplyWaitEntry)));
	return sz;
}

void
cluster_ges_reply_wait_shmem_init(void)
{
	bool found;
	HASHCTL hctl;

	reply_wait_state = (ClusterGesReplyWaitShared *)ShmemInitStruct(
		"pgrac cluster ges reply wait", MAXALIGN(sizeof(ClusterGesReplyWaitShared)), &found);

	if (!IsUnderPostmaster) {
		LWLockInitialize(&reply_wait_state->lwlock, LWTRANCHE_CLUSTER_GES_REPLY_WAIT);
		pg_atomic_init_u64(&reply_wait_state->reply_wait_table_active, 0);
		pg_atomic_init_u64(&reply_wait_state->reply_late_drop_count, 0);
		pg_atomic_init_u64(&reply_wait_state->release_ack_count, 0);
		pg_atomic_init_u64(&reply_wait_state->sweep_deleted_count, 0);
		pg_atomic_init_u64(&reply_wait_state->request_id_seq, 0);
	}

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.keysize = sizeof(GesReplyWaitKey);
	hctl.entrysize = sizeof(GesReplyWaitEntry);
	reply_wait_htab
		= ShmemInitHash("pgrac cluster ges reply wait htab", CLUSTER_GES_REPLY_WAIT_DEFAULT_MAX,
						CLUSTER_GES_REPLY_WAIT_DEFAULT_MAX, &hctl, HASH_ELEM | HASH_BLOBS);
}

void
cluster_ges_reply_wait_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_ges_reply_wait_region);
}

/*
 * spec-5.16 (Rule 8.A) — hand out the next node-global request_id (never 0).
 *
 *	Used by the lock-acquire path (cluster_lock_acquire.c) so the reply-wait
 *	5-tuple key is unique node-wide, never colliding across backends.  Before
 *	shmem is attached (or on a non-cluster path that somehow asks), fall back to a
 *	backend-local counter — that path creates no reply-wait entry, so per-backend
 *	uniqueness is sufficient there and a 0 (sentinel) is never returned.
 */
uint64
cluster_ges_reply_wait_next_request_id(void)
{
	static uint64 local_fallback = 0;

	if (reply_wait_state == NULL)
		return ++local_fallback;
	return pg_atomic_fetch_add_u64(&reply_wait_state->request_id_seq, 1) + 1;
}


/* ============================================================
 * HTAB mutator / accessor API (HC17).
 * ============================================================ */

/*
 * spec-5.16 (orphan-grant, Rule 8.A) — reclaim one slot under table-full pressure
 * by evicting the oldest ABANDONED tombstone.  The reply-wait table MUST always be
 * able to admit a LIVE request: a tombstone is only an optimization that lets the
 * waiter side auto-release a late orphan GRANT promptly, but the master-side P6
 * sweep is the real orphan backstop (see cluster_ges.c ges_abandon_wait_or_release
 * NOTES), so dropping a tombstone here at worst defers one orphan's cleanup to that
 * backstop — strictly better than fail-closing a live request with "reply wait
 * table full" (which is exactly what a node-leave burst would otherwise trigger,
 * as every in-flight request to the departed master times out at once).  A live
 * (non-abandoned) waiter is NEVER evicted — a backend is sleeping on its CV.
 *
 *	Caller holds reply_wait_state->lwlock EXCLUSIVE.  Returns true if a tombstone
 *	was evicted (a slot is now free).  Two-pass (collect key, then HASH_REMOVE) to
 *	avoid mutating the HTAB mid-iteration.
 */
static bool
ges_reply_wait_evict_oldest_tombstone_locked(void)
{
	HASH_SEQ_STATUS scan;
	GesReplyWaitEntry *entry;
	GesReplyWaitKey victim_key;
	TimestampTz oldest = 0;
	bool have_victim = false;
	bool found;

	hash_seq_init(&scan, reply_wait_htab);
	while ((entry = (GesReplyWaitEntry *)hash_seq_search(&scan)) != NULL) {
		if (!entry->abandoned)
			continue; /* live waiter — never evict */
		if (!have_victim || entry->deadline < oldest) {
			oldest = entry->deadline;
			victim_key = entry->key;
			have_victim = true;
		}
	}
	if (!have_victim)
		return false; /* table is full of live waiters — genuinely full */

	(void)hash_search(reply_wait_htab, &victim_key, HASH_REMOVE, &found);
	if (found) {
		pg_atomic_fetch_sub_u64(&reply_wait_state->reply_wait_table_active, 1);
		pg_atomic_fetch_add_u64(&reply_wait_state->sweep_deleted_count, 1);
	}
	return true;
}

/* spec-5.16 — one-shot LOG when the reply-wait table first sweeps under pressure
 * (Rule 17:  capacity / degradation event → counter (sweep_deleted_count) +
 * LOG-once, not a per-event WARNING flood).  Backend-local flag. */
static void
ges_reply_wait_log_pressure_once(void)
{
	static bool logged = false;

	if (logged)
		return;
	logged = true;
	ereport(LOG, (errmsg_internal("cluster GES reply-wait table full; evicting orphan tombstones"
								  " to admit live requests (master-side P6 sweep remains the orphan"
								  " backstop)")));
}

GesReplyWaitEntry *
cluster_ges_reply_wait_insert(const GesReplyWaitKey *key, TimestampTz deadline)
{
	GesReplyWaitEntry *entry;
	bool found;
	bool evicted = false;

	Assert(key != NULL);
	if (reply_wait_state == NULL || reply_wait_htab == NULL)
		return NULL; /* shmem not yet initialized — caller fails closed */

	LWLockAcquire(&reply_wait_state->lwlock, LW_EXCLUSIVE);

	/*
	 * Cap check.  When the table is full, first try to reclaim a slot by evicting
	 * an orphan tombstone (spec-5.16) — a live request must never be fail-closed
	 * (SQLSTATE 53R71) because opportunistic tombstones filled the table.  Only if
	 * no tombstone can be evicted (the table is genuinely full of live waiters) do
	 * we fail closed.
	 */
	if (hash_get_num_entries(reply_wait_htab) >= CLUSTER_GES_REPLY_WAIT_DEFAULT_MAX) {
		if (!ges_reply_wait_evict_oldest_tombstone_locked()) {
			LWLockRelease(&reply_wait_state->lwlock);
			return NULL;
		}
		evicted = true;
	}

	entry = (GesReplyWaitEntry *)hash_search(reply_wait_htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		/* HTAB internal slots exhausted despite the count check — evict + retry once. */
		if (ges_reply_wait_evict_oldest_tombstone_locked()) {
			evicted = true;
			entry = (GesReplyWaitEntry *)hash_search(reply_wait_htab, key, HASH_ENTER_NULL, &found);
		}
		if (entry == NULL) {
			LWLockRelease(&reply_wait_state->lwlock);
			return NULL;
		}
	}
	if (found) {
		/*
		 * Duplicate 5-tuple — should not happen in normal flow because
		 * request_id is per-backend monotonic + (source, dest, opcode,
		 * epoch) further discriminates.  Treat as caller bug.
		 */
		LWLockRelease(&reply_wait_state->lwlock);
		return NULL;
	}

	ConditionVariableInit(&entry->cv);
	entry->reject_reason = 0;
	entry->reply_opcode = 0;
	entry->deadline = deadline;
	entry->ready = false;
	entry->abandoned = false;

	pg_atomic_fetch_add_u64(&reply_wait_state->reply_wait_table_active, 1);

	LWLockRelease(&reply_wait_state->lwlock);

	/* LOG-once outside the lock (Rule 17 — no log I/O under the LWLock). */
	if (evicted)
		ges_reply_wait_log_pressure_once();

	return entry;
}

GesReplyWaitEntry *
cluster_ges_reply_wait_lookup(const GesReplyWaitKey *key)
{
	GesReplyWaitEntry *entry;

	if (reply_wait_state == NULL || reply_wait_htab == NULL)
		return NULL;

	LWLockAcquire(&reply_wait_state->lwlock, LW_SHARED);
	entry = (GesReplyWaitEntry *)hash_search(reply_wait_htab, key, HASH_FIND, NULL);
	LWLockRelease(&reply_wait_state->lwlock);

	return entry; /* NULL = HC17 late reply path */
}

void
cluster_ges_reply_wait_wake(GesReplyWaitEntry *entry, uint32 reply_opcode, uint32 reject_reason)
{
	Assert(entry != NULL);
	/*
	 * Set the verdict fields and ready flag, then broadcast the
	 * per-entry CV.  Waiter loops on `ready` (CV spurious wake
	 * protection).  No lwlock needed — the entry pointer is stable
	 * for as long as the waiter has not yet deleted the entry, and
	 * the waiter only deletes after observing ready==true.
	 */
	entry->reply_opcode = reply_opcode;
	entry->reject_reason = reject_reason;
	pg_write_barrier();
	entry->ready = true;
	ConditionVariableBroadcast(&entry->cv);
}

GesReplyDeliverResult
cluster_ges_reply_wait_deliver(const GesReplyWaitKey *key, uint32 reply_opcode,
							   uint32 reject_reason)
{
	GesReplyWaitEntry *entry;
	GesReplyWaitEntry *woke = NULL;
	GesReplyDeliverResult result;
	ClusterXpScope xp_wake; /* PGRAC: spec-5.59 D2 profiling */

	/* PGRAC: spec-5.59 D2 profiling — deliver + wake service-time (LMON side) */
	cluster_xp_begin(&xp_wake, CLXP_W_GES_WAKE);

	if (reply_wait_state == NULL || reply_wait_htab == NULL) {
		cluster_xp_end(&xp_wake); /* PGRAC: spec-5.59 D2 profiling */
		return GES_REPLY_DELIVER_NO_WAITER;
	}

	LWLockAcquire(&reply_wait_state->lwlock, LW_EXCLUSIVE);
	entry = (GesReplyWaitEntry *)hash_search(reply_wait_htab, key, HASH_FIND, NULL);
	if (entry == NULL) {
		/* No entry: the waiter already succeeded and deleted it (this also covers a
		 * retransmit-duplicate GRANT for that succeeded request). */
		result = GES_REPLY_DELIVER_NO_WAITER;
	} else if (entry->abandoned) {
		/* Tombstone: the waiter gave up at the GES timeout.  A GRANT landing here is
		 * an ORPHAN — remove the tombstone and let the caller release it.  A REJECT
		 * grants nothing, so just drop it. */
		bool found;

		result = (reply_opcode == (uint32)GES_REPLY_OPCODE_GRANT) ? GES_REPLY_DELIVER_ORPHAN
																  : GES_REPLY_DELIVER_NO_WAITER;
		(void)hash_search(reply_wait_htab, key, HASH_REMOVE, &found);
		if (found)
			pg_atomic_fetch_sub_u64(&reply_wait_state->reply_wait_table_active, 1);
	} else {
		/* Live waiter: store the verdict + ready under the lock; broadcast the CV
		 * AFTER releasing it (the waiter re-checks `ready`, so the broadcast is only a
		 * wakeup hint — mirrors the lock-free cluster_ges_reply_wait_wake contract). */
		entry->reply_opcode = reply_opcode;
		entry->reject_reason = reject_reason;
		pg_write_barrier();
		entry->ready = true;
		woke = entry;
		result = GES_REPLY_DELIVER_WOKE;
	}
	LWLockRelease(&reply_wait_state->lwlock);

	if (woke != NULL)
		ConditionVariableBroadcast(&woke->cv);
	cluster_xp_end(&xp_wake); /* PGRAC: spec-5.59 D2 profiling */
	return result;
}

bool
cluster_ges_reply_wait_mark_abandoned(const GesReplyWaitKey *key, TimestampTz tombstone_deadline)
{
	GesReplyWaitEntry *entry;
	bool was_granted = false;

	if (reply_wait_state == NULL || reply_wait_htab == NULL)
		return false;

	LWLockAcquire(&reply_wait_state->lwlock, LW_EXCLUSIVE);
	entry = (GesReplyWaitEntry *)hash_search(reply_wait_htab, key, HASH_FIND, NULL);
	if (entry != NULL) {
		if (entry->ready && entry->reply_opcode == (uint32)GES_REPLY_OPCODE_GRANT) {
			/* A GRANT raced just ahead of this timeout — the master already granted
			 * us.  Remove the entry; the caller (backend context) releases the
			 * unwanted grant itself. */
			bool found;

			was_granted = true;
			(void)hash_search(reply_wait_htab, key, HASH_REMOVE, &found);
			if (found)
				pg_atomic_fetch_sub_u64(&reply_wait_state->reply_wait_table_active, 1);
		} else {
			/* Keep as a tombstone so a later GRANT is recognized as an orphan and
			 * auto-released by the reply handler.  Re-arm the deadline to the short
			 * TTL so the timeout sweep reaps it if no grant ever lands. */
			entry->abandoned = true;
			entry->deadline = tombstone_deadline;
		}
	}
	LWLockRelease(&reply_wait_state->lwlock);
	return was_granted;
}

void
cluster_ges_reply_wait_delete(const GesReplyWaitKey *key)
{
	bool found;

	if (reply_wait_state == NULL || reply_wait_htab == NULL)
		return;

	LWLockAcquire(&reply_wait_state->lwlock, LW_EXCLUSIVE);
	(void)hash_search(reply_wait_htab, key, HASH_REMOVE, &found);
	if (found)
		pg_atomic_fetch_sub_u64(&reply_wait_state->reply_wait_table_active, 1);
	LWLockRelease(&reply_wait_state->lwlock);
}

int
cluster_ges_reply_wait_sweep_timeout(TimestampTz now)
{
	HASH_SEQ_STATUS scan;
	GesReplyWaitEntry *entry;
	GesReplyWaitKey victim_keys[64];
	int n_victims = 0;
	int total_swept = 0;

	if (reply_wait_state == NULL || reply_wait_htab == NULL)
		return 0;

	/*
	 * Two-pass to avoid mutating the HTAB while iterating.  Cap each
	 * pass at 64 victims so the lock hold time is bounded; LMON tick
	 * will call again next iteration if more remain.
	 */
	LWLockAcquire(&reply_wait_state->lwlock, LW_EXCLUSIVE);

	hash_seq_init(&scan, reply_wait_htab);
	while ((entry = (GesReplyWaitEntry *)hash_seq_search(&scan)) != NULL) {
		/*
		 * spec-5.16 (orphan-grant, Rule 8.A) — reclaim ONLY abandoned tombstones,
		 * never a live waiter whose deadline merely elapsed.  A live (non-abandoned)
		 * waiter owns its own entry while it sleeps on entry->cv; it processes its
		 * deadline itself (cluster_ges.c ges_send_request_opcode_and_wait caps the
		 * CV sleep at the remaining time, then deletes or tombstones).  Sweeping a
		 * live waiter's entry here would HASH_REMOVE it out from under a sleeping
		 * backend and let the freed slot be reused — a use-after-reuse race on the
		 * CV.  A tombstone, by contrast, is set ONLY after its backend has already
		 * returned (no one sleeps on it), so reclaiming it is race-free.
		 */
		if (entry->abandoned && entry->deadline != 0 && entry->deadline <= now) {
			if (n_victims < (int)lengthof(victim_keys))
				victim_keys[n_victims++] = entry->key;
		}
	}

	for (int i = 0; i < n_victims; i++) {
		bool found;

		(void)hash_search(reply_wait_htab, &victim_keys[i], HASH_REMOVE, &found);
		if (found) {
			pg_atomic_fetch_sub_u64(&reply_wait_state->reply_wait_table_active, 1);
			pg_atomic_fetch_add_u64(&reply_wait_state->sweep_deleted_count, 1);
			total_swept++;
		}
	}

	LWLockRelease(&reply_wait_state->lwlock);
	return total_swept;
}


/* ============================================================
 * Counter accessors (cluster_debug dump_ges surface).
 * ============================================================ */

uint64
cluster_ges_reply_wait_table_active_count(void)
{
	if (reply_wait_state == NULL)
		return 0;
	return pg_atomic_read_u64(&reply_wait_state->reply_wait_table_active);
}

uint64
cluster_ges_reply_late_drop_count(void)
{
	if (reply_wait_state == NULL)
		return 0;
	return pg_atomic_read_u64(&reply_wait_state->reply_late_drop_count);
}

uint64
cluster_ges_release_ack_count(void)
{
	if (reply_wait_state == NULL)
		return 0;
	return pg_atomic_read_u64(&reply_wait_state->release_ack_count);
}

void
cluster_ges_inc_release_ack(void)
{
	if (reply_wait_state != NULL)
		pg_atomic_fetch_add_u64(&reply_wait_state->release_ack_count, 1);
}

void
cluster_ges_inc_reply_late_drop(void)
{
	if (reply_wait_state != NULL)
		pg_atomic_fetch_add_u64(&reply_wait_state->reply_late_drop_count, 1);
}
