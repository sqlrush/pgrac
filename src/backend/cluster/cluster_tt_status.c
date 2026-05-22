/*-------------------------------------------------------------------------
 *
 * cluster_tt_status.c
 *	  pgrac cluster Undo Transaction Table (TT) status overlay —
 *	  bounded in-memory HTAB keyed by ClusterTTStatusKey (24B exact key).
 *
 *	  spec-3.1 D2 (NEW).
 *
 *	  This file implements the foundation overlay that backs
 *	  cluster_tt_status_lookup_exact / install_local / flush_all /
 *	  generation.  Install path (D5/D6) and ITL reader (D4) live in
 *	  separate files.
 *
 *	  Concurrency:
 *	    - single LWLock (LWTRANCHE_CLUSTER_TT_STATUS) guards HTAB +
 *	      generation counter + counters.  SHARED for lookup_exact;
 *	      EXCLUSIVE for install/flush_all/evict (HC182).
 *	    - HTAB sized at postmaster startup from
 *	      cluster.tt_status_overlay_max_entries (PGC_POSTMASTER startup
 *	      capacity; TTL is read each lookup).
 *
 *	  TTL / generation:
 *	    - each entry stamps install_ts; lookup_exact ages out entries
 *	      older than cluster.tt_status_overlay_ttl_ms (HC181 fail-closed
 *	      — miss returns UNKNOWN, never silent-fallback CLOG; L176).
 *	    - reconfig epoch bump (D7 callsite in cluster_reconfig.c)
 *	      invokes cluster_tt_status_flush_all(new_epoch);
 *	      generation++; future lookups for old epoch return UNKNOWN
 *	      naturally (HC182).
 *
 *	  Counters (observability):
 *	    install / lookup_hit / lookup_miss / evict / flush /
 *	    ambiguous_raw_xid_reject (always zero in spec-3.1 — no raw xid
 *	    API; placeholder for future audit) / self_consumer_hit (N7).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.1-cluster-xid-status-foundation.md
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_status.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"
#include "cluster/cluster_tt_status.h"

/*
 * ClusterTTOverlayEntry -- HTAB value type.
 *
 * Key is the ClusterTTStatusKey defined in the public header; value
 * carries status / commit_scn / install_ts / status_epoch.
 */
typedef struct ClusterTTOverlayEntry {
	ClusterTTStatusKey key; /* HTAB key (24B; HASH_BLOBS) */

	ClusterTTStatus status;
	SCN commit_scn;
	uint32 status_epoch;
	TimestampTz install_ts;
} ClusterTTOverlayEntry;

/*
 * ClusterTTStatusShmem -- shmem header (single struct, then HTAB
 * lives in its own ShmemInitHash region).
 */
typedef struct ClusterTTStatusShmem {
	pg_atomic_uint64 generation; /* bumped on flush_all */

	/* Counters (atomic, low-frequency reads from SQL views). */
	pg_atomic_uint64 install_count;
	pg_atomic_uint64 lookup_hit_count;
	pg_atomic_uint64 lookup_miss_count;
	pg_atomic_uint64 evict_count;
	pg_atomic_uint64 flush_count;
	pg_atomic_uint64 self_consumer_hit_count;
	pg_atomic_uint64 ambiguous_raw_xid_reject_count;
	pg_atomic_uint64 evict_fail_count;
} ClusterTTStatusShmem;

#ifdef USE_PGRAC_CLUSTER

static HTAB *ClusterTTStatusHTAB = NULL;
static LWLock *ClusterTTStatusLock = NULL;
static ClusterTTStatusShmem *ClusterTTStatusState = NULL;

/* ------------------------------------------------------------ */
/* shmem layout helpers                                         */
/* ------------------------------------------------------------ */

Size
cluster_tt_status_shmem_size(void)
{
	Size sz;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;

	sz = MAXALIGN(sizeof(ClusterTTStatusShmem));
	sz = add_size(sz, MAXALIGN(sizeof(LWLockPadded)));
	sz = add_size(sz, hash_estimate_size(cluster_tt_status_overlay_max_entries,
										 sizeof(ClusterTTOverlayEntry)));
	return sz;
}

void
cluster_tt_status_shmem_init(void)
{
	HASHCTL info;
	bool found;
	LWLockPadded *lockblock;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	/* State header + counters. */
	ClusterTTStatusState = (ClusterTTStatusShmem *)ShmemInitStruct(
		"ClusterTTStatusState", MAXALIGN(sizeof(ClusterTTStatusShmem)), &found);
	if (!found) {
		pg_atomic_init_u64(&ClusterTTStatusState->generation, 1);
		pg_atomic_init_u64(&ClusterTTStatusState->install_count, 0);
		pg_atomic_init_u64(&ClusterTTStatusState->lookup_hit_count, 0);
		pg_atomic_init_u64(&ClusterTTStatusState->lookup_miss_count, 0);
		pg_atomic_init_u64(&ClusterTTStatusState->evict_count, 0);
		pg_atomic_init_u64(&ClusterTTStatusState->flush_count, 0);
		pg_atomic_init_u64(&ClusterTTStatusState->self_consumer_hit_count, 0);
		pg_atomic_init_u64(&ClusterTTStatusState->ambiguous_raw_xid_reject_count, 0);
		pg_atomic_init_u64(&ClusterTTStatusState->evict_fail_count, 0);
	}

	/* Lock. */
	lockblock = (LWLockPadded *)ShmemInitStruct("ClusterTTStatusLock",
												MAXALIGN(sizeof(LWLockPadded)), &found);
	if (!found)
		LWLockInitialize(&lockblock->lock, LWTRANCHE_CLUSTER_TT_STATUS);
	ClusterTTStatusLock = &lockblock->lock;

	/* HTAB.  ClusterTTStatusKey is 24B blob — HASH_BLOBS is fine.
	 * Use memset rather than PG's MemSet macro:  cppcheck 2.13 flags
	 * MemSet's internal _stop pointer as constVariablePointer (known
	 * false positive on the PG macro expansion). */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(ClusterTTStatusKey);
	info.entrysize = sizeof(ClusterTTOverlayEntry);
	info.num_partitions = 1;
	ClusterTTStatusHTAB
		= ShmemInitHash("ClusterTTStatusOverlay", cluster_tt_status_overlay_max_entries,
						cluster_tt_status_overlay_max_entries, &info, HASH_ELEM | HASH_BLOBS);
}

/* ------------------------------------------------------------ */
/* shmem region registration                                    */
/* ------------------------------------------------------------ */

static const ClusterShmemRegion cluster_tt_status_region = {
	.name = "pgrac cluster tt status overlay",
	.size_fn = cluster_tt_status_shmem_size,
	.init_fn = cluster_tt_status_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_tt_status",
	.reserved_flags = 0,
};

void
cluster_tt_status_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_tt_status_region);
}

/* ------------------------------------------------------------ */
/* private helpers                                              */
/* ------------------------------------------------------------ */

/*
 * is_entry_fresh -- check install_ts against now / TTL.  Caller holds lock.
 */
static bool
is_entry_fresh(const ClusterTTOverlayEntry *e, TimestampTz now)
{
	int64 age_us;

	if (cluster_tt_status_overlay_ttl_ms <= 0)
		return true;
	age_us = now - e->install_ts;
	return age_us < ((int64)cluster_tt_status_overlay_ttl_ms * 1000);
}

/* ------------------------------------------------------------ */
/* public API                                                   */
/* ------------------------------------------------------------ */

bool
cluster_tt_status_lookup_exact(const ClusterTTStatusKey *key, ClusterTTStatusResult *result)
{
	const ClusterTTOverlayEntry *e;
	TimestampTz now;
	uint32 current_epoch;

	/* Explicit null guards (also satisfies CI cppcheck nullPointerRedundantCheck;
	 * Assert-only is debug-build-only and cppcheck strips it). */
	if (key == NULL || result == NULL)
		return false;

	/* HC181:  always set fail-closed sentinel first. */
	result->status = CLUSTER_TT_STATUS_UNKNOWN;
	result->commit_scn = InvalidScn;
	result->status_epoch = 0;
	result->authoritative = false;

	if (!cluster_enabled || ClusterTTStatusHTAB == NULL)
		return false;

	/* HC182:  reject lookups for an epoch newer/older than current. */
	current_epoch = (uint32)cluster_epoch_get_current();
	if (key->cluster_epoch != current_epoch) {
		pg_atomic_fetch_add_u64(&ClusterTTStatusState->lookup_miss_count, 1);
		return false;
	}

	now = GetCurrentTimestamp();

	LWLockAcquire(ClusterTTStatusLock, LW_SHARED);
	e = (ClusterTTOverlayEntry *)hash_search(ClusterTTStatusHTAB, key, HASH_FIND, NULL);
	if (e == NULL || !is_entry_fresh(e, now)) {
		LWLockRelease(ClusterTTStatusLock);
		pg_atomic_fetch_add_u64(&ClusterTTStatusState->lookup_miss_count, 1);
		return false;
	}

	result->status = e->status;
	result->commit_scn = e->commit_scn;
	result->status_epoch = e->status_epoch;
	result->authoritative = true;

	LWLockRelease(ClusterTTStatusLock);
	pg_atomic_fetch_add_u64(&ClusterTTStatusState->lookup_hit_count, 1);
	return true;
}

void
cluster_tt_status_install_local(const ClusterTTStatusKey *key, ClusterTTStatus status,
								SCN commit_scn)
{
	ClusterTTOverlayEntry *e;
	bool found;

	Assert(key != NULL);

	if (!cluster_enabled || ClusterTTStatusHTAB == NULL)
		return;

	LWLockAcquire(ClusterTTStatusLock, LW_EXCLUSIVE);

	e = (ClusterTTOverlayEntry *)hash_search(ClusterTTStatusHTAB, key, HASH_ENTER_NULL, &found);
	if (e == NULL) {
		/*
		 * HTAB full and no eviction implemented in foundation spec.  Bump
		 * evict_fail_count (defensive; spec-3.1 D9 T22 covers).  Caller
		 * still proceeds — overlay is best-effort cache, miss returns
		 * UNKNOWN per HC181.
		 */
		LWLockRelease(ClusterTTStatusLock);
		pg_atomic_fetch_add_u64(&ClusterTTStatusState->evict_fail_count, 1);
		ereport(WARNING, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						  errmsg("cluster tt status overlay full; install dropped"),
						  errhint("Raise cluster.tt_status_overlay_max_entries or lower TTL.")));
		return;
	}

	e->status = status;
	e->commit_scn = commit_scn;
	e->status_epoch = key->cluster_epoch;
	e->install_ts = GetCurrentTimestamp();

	LWLockRelease(ClusterTTStatusLock);

	pg_atomic_fetch_add_u64(&ClusterTTStatusState->install_count, 1);
}

void
cluster_tt_status_flush_all(uint32 new_epoch)
{
	HASH_SEQ_STATUS hseq;
	const ClusterTTOverlayEntry *e;
	uint64 removed = 0;

	(void)new_epoch;

	if (!cluster_enabled || ClusterTTStatusHTAB == NULL)
		return;

	LWLockAcquire(ClusterTTStatusLock, LW_EXCLUSIVE);

	hash_seq_init(&hseq, ClusterTTStatusHTAB);
	while ((e = (ClusterTTOverlayEntry *)hash_seq_search(&hseq)) != NULL) {
		hash_search(ClusterTTStatusHTAB, &e->key, HASH_REMOVE, NULL);
		removed++;
	}

	pg_atomic_fetch_add_u64(&ClusterTTStatusState->generation, 1);
	pg_atomic_fetch_add_u64(&ClusterTTStatusState->flush_count, 1);
	pg_atomic_fetch_add_u64(&ClusterTTStatusState->evict_count, removed);

	LWLockRelease(ClusterTTStatusLock);
}

uint64
cluster_tt_status_generation(void)
{
	if (!cluster_enabled || ClusterTTStatusState == NULL)
		return 0;
	return pg_atomic_read_u64(&ClusterTTStatusState->generation);
}

/*
 * cluster_tt_status_bump_self_consumer_hit -- internal counter bump used
 * by D6 commit hook to record the runtime self-consumer lookup
 * (spec-3.1 v0.4 N7).  Only D5/D6 should call this.
 */
void
cluster_tt_status_bump_self_consumer_hit(void)
{
	if (!cluster_enabled || ClusterTTStatusState == NULL)
		return;
	pg_atomic_fetch_add_u64(&ClusterTTStatusState->self_consumer_hit_count, 1);
}

/* ------------------------------------------------------------ */
/* counter getters (exposed via pg_cluster_state)               */
/* ------------------------------------------------------------ */

#define CLUSTER_TT_STATUS_COUNTER_GETTER(name)                                                     \
	uint64 cluster_tt_status_get_##name(void)                                                      \
	{                                                                                              \
		if (!cluster_enabled || ClusterTTStatusState == NULL)                                      \
			return 0;                                                                              \
		return pg_atomic_read_u64(&ClusterTTStatusState->name);                                    \
	}

CLUSTER_TT_STATUS_COUNTER_GETTER(install_count)
CLUSTER_TT_STATUS_COUNTER_GETTER(lookup_hit_count)
CLUSTER_TT_STATUS_COUNTER_GETTER(lookup_miss_count)
CLUSTER_TT_STATUS_COUNTER_GETTER(evict_count)
CLUSTER_TT_STATUS_COUNTER_GETTER(flush_count)
CLUSTER_TT_STATUS_COUNTER_GETTER(self_consumer_hit_count)
CLUSTER_TT_STATUS_COUNTER_GETTER(evict_fail_count)

#else /* !USE_PGRAC_CLUSTER */

Size
cluster_tt_status_shmem_size(void)
{
	return 0;
}

void
cluster_tt_status_shmem_init(void)
{}

void
cluster_tt_status_shmem_register(void)
{}

bool
cluster_tt_status_lookup_exact(const ClusterTTStatusKey *key, ClusterTTStatusResult *result)
{
	if (result != NULL) {
		result->status = CLUSTER_TT_STATUS_UNKNOWN;
		result->commit_scn = 0;
		result->status_epoch = 0;
		result->authoritative = false;
	}
	(void)key;
	return false;
}

void
cluster_tt_status_install_local(const ClusterTTStatusKey *key, ClusterTTStatus status,
								SCN commit_scn)
{
	(void)key;
	(void)status;
	(void)commit_scn;
}

void
cluster_tt_status_flush_all(uint32 new_epoch)
{
	(void)new_epoch;
}

uint64
cluster_tt_status_generation(void)
{
	return 0;
}

void
cluster_tt_status_bump_self_consumer_hit(void)
{}

#define CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(name)                                                \
	uint64 cluster_tt_status_get_##name(void)                                                      \
	{                                                                                              \
		return 0;                                                                                  \
	}

CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(install_count)
CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(lookup_hit_count)
CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(lookup_miss_count)
CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(evict_count)
CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(flush_count)
CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(self_consumer_hit_count)
CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(evict_fail_count)

#endif /* USE_PGRAC_CLUSTER */
