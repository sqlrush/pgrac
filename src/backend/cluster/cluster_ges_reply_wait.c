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

#include "cluster/cluster_ges_reply_wait.h"
#include "cluster/cluster_shmem.h"


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


/* ============================================================
 * HTAB mutator / accessor API (HC17).
 * ============================================================ */

GesReplyWaitEntry *
cluster_ges_reply_wait_insert(const GesReplyWaitKey *key, TimestampTz deadline)
{
	GesReplyWaitEntry *entry;
	bool found;

	Assert(key != NULL);
	if (reply_wait_state == NULL || reply_wait_htab == NULL)
		return NULL; /* shmem not yet initialized — caller fails closed */

	LWLockAcquire(&reply_wait_state->lwlock, LW_EXCLUSIVE);

	/* Cap check — fail-closed at SQLSTATE 53R71 from caller. */
	if (hash_get_num_entries(reply_wait_htab) >= CLUSTER_GES_REPLY_WAIT_DEFAULT_MAX) {
		LWLockRelease(&reply_wait_state->lwlock);
		return NULL;
	}

	entry = (GesReplyWaitEntry *)hash_search(reply_wait_htab, key, HASH_ENTER_NULL, &found);
	if (entry == NULL) {
		LWLockRelease(&reply_wait_state->lwlock);
		return NULL;
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

	pg_atomic_fetch_add_u64(&reply_wait_state->reply_wait_table_active, 1);

	LWLockRelease(&reply_wait_state->lwlock);
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
		if (entry->deadline != 0 && entry->deadline <= now) {
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
