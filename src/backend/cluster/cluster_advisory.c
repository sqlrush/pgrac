/*-------------------------------------------------------------------------
 *
 * cluster_advisory.c
 *	  UL user lock: cross-node advisory (user) lock scope + observability.
 *
 *	  See cluster_advisory.h for the layering rationale.  This module is
 *	  deliberately small: cross-node advisory mutual exclusion is delivered
 *	  by lifting LOCKTAG_ADVISORY through the existing gate
 *	  (cluster_lock_should_globalize, spec-5.5 D1) onto the spec-5.3 GES
 *	  acquire/release substrate.  Session lifetime reuses PG's native session
 *	  lock-owner; this file only derives the session-vs-xact scope a LOCALLOCK
 *	  carries (for the backend-exit release label) and owns the UL counters.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_advisory.c
 *
 * NOTES
 *	  Spec: spec-5.5-ul-advisory-lock-cross-node.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_advisory.h"
#include "cluster/cluster_shmem.h"
#include "port/atomics.h"
#include "storage/lock.h"
#include "storage/shmem.h"

/*
 * spec-5.5 D8 — UL observability counters live in a dedicated lock-free shmem
 * region (mirror the cluster_ges defer-counter region: atomics only, no LWLock).
 */
typedef struct ClusterAdvisorySharedState {
	pg_atomic_uint64 counters[CLUSTER_ADVISORY_COUNTER_COUNT];
} ClusterAdvisorySharedState;

static ClusterAdvisorySharedState *cluster_advisory_state = NULL;


/*
 * cluster_advisory_locallock_is_session_scoped -- spec-5.5 D3 / §3.2 L5.
 *
 *	A LOCALLOCK is session-scoped iff at least one of its lock-owners is the
 *	PG session owner (lockOwners[i].owner == NULL).  The backend-exit
 *	LockReleaseAll(allLocks=true) drain uses this to label the cluster release
 *	correctly; the xact-end LockReleaseAll(allLocks=false) path never reaches
 *	the cluster hook for a session-owned locallock (it keeps the holder alive),
 *	so a mixed session+xact same-key locallock that does reach the backend-exit
 *	drain is correctly classed session-scoped (the session owner is what
 *	survived the transaction).
 */
bool
cluster_advisory_locallock_is_session_scoped(const LOCALLOCK *locallock)
{
	int i;

	if (locallock == NULL)
		return false;

	for (i = 0; i < locallock->numLockOwners; i++) {
		if (locallock->lockOwners[i].owner == NULL)
			return true;
	}
	return false;
}


/* ============================================================
 * spec-5.5 D8 — shmem region lifecycle (mirror cluster_ges pattern).
 * ============================================================ */

Size
cluster_advisory_shmem_size(void)
{
	return sizeof(ClusterAdvisorySharedState);
}

void
cluster_advisory_shmem_init(void)
{
	bool found;

	cluster_advisory_state
		= ShmemInitStruct("pgrac cluster advisory", cluster_advisory_shmem_size(), &found);
	if (!found) {
		int i;

		for (i = 0; i < CLUSTER_ADVISORY_COUNTER_COUNT; i++)
			pg_atomic_init_u64(&cluster_advisory_state->counters[i], 0);
	}
}

static const ClusterShmemRegion cluster_advisory_region = {
	.name = "pgrac cluster advisory",
	.size_fn = cluster_advisory_shmem_size,
	.init_fn = cluster_advisory_shmem_init,
	.lwlock_count = 0, /* atomics only (L106 inherit) */
	.owner_subsys = "cluster_advisory",
	.reserved_flags = 0,
};

void
cluster_advisory_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_advisory_region);
}


/* ============================================================
 * spec-5.5 D8 — counter mutate + read (NULL/uninit-safe).
 * ============================================================ */

void
cluster_advisory_counter_inc(ClusterAdvisoryCounter which)
{
	if (cluster_advisory_state == NULL || (int)which < 0
		|| (int)which >= CLUSTER_ADVISORY_COUNTER_COUNT)
		return;
	pg_atomic_fetch_add_u64(&cluster_advisory_state->counters[which], 1);
}

uint64
cluster_advisory_counter_read(ClusterAdvisoryCounter which)
{
	if (cluster_advisory_state == NULL || (int)which < 0
		|| (int)which >= CLUSTER_ADVISORY_COUNTER_COUNT)
		return 0;
	return pg_atomic_read_u64(&cluster_advisory_state->counters[which]);
}
