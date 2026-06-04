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

#include "access/transam.h" /* spec-3.11 D5/C1b: TransactionIdDidCommit */
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
#include "cluster/cluster_tt_durable.h" /* spec-3.11 D5: overlay-miss durable lookup */
#include "cluster/cluster_tt_slot.h"	/* cluster_tt_slot_id_to_offset, TT_SLOTS_PER_SEGMENT */
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

	/*
	 * PGRAC (spec-3.5): parent_key for SUBCOMMITTED entries.
	 *
	 * Only populated when status == CLUSTER_TT_STATUS_SUBCOMMITTED.
	 * has_parent_key=false / parent_key zeroed for all other states.
	 * Reader follows this chain via cluster_tt_status_lookup_exact
	 * (bounded by cluster.subtrans_max_chain_depth).
	 */
	bool has_parent_key;
	ClusterTTStatusKey parent_key;
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

	/* PGRAC (spec-3.5): SUBCOMMITTED path counters. */
	pg_atomic_uint64 subcommitted_install_count;
	pg_atomic_uint64 subcommitted_lookup_hit_count;
	pg_atomic_uint64 parent_chain_follow_count;
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
		/* PGRAC (spec-3.5) */
		pg_atomic_init_u64(&ClusterTTStatusState->subcommitted_install_count, 0);
		pg_atomic_init_u64(&ClusterTTStatusState->subcommitted_lookup_hit_count, 0);
		pg_atomic_init_u64(&ClusterTTStatusState->parent_chain_follow_count, 0);
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

static void
note_overlay_full(const char *dropped_kind)
{
	uint64 old_count;

	Assert(ClusterTTStatusState != NULL);
	old_count = pg_atomic_fetch_add_u64(&ClusterTTStatusState->evict_fail_count, 1);
	if (old_count == 0)
		ereport(LOG, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					  errmsg("cluster tt status overlay full; %s dropped", dropped_kind),
					  errhint("Raise cluster.tt_status_overlay_max_entries or lower TTL.")));
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
	/* PGRAC (spec-3.5): parent_key sentinel — must be zero for non-SUBCOMMITTED. */
	result->has_parent_key = false;
	memset(&result->parent_key, 0, sizeof(result->parent_key));

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

		/*
		 * spec-3.11 D5/C3: overlay miss -> own-instance durable TT lookup.  The
		 * durable TT slot in the undo segment header survives overlay eviction
		 * and restart while still bound to local_xid, so this retires the
		 * fail-closed those cases otherwise force.  Only own-instance (origin ==
		 * local node); a remote origin's durable TT is Stage 4 (Cache Fusion).
		 *
		 * C1b (规则 8.A): the durable slot is stamped at pre-commit (spec-3.11
		 * C1), so it alone does not prove the xact committed -- confirm via CLOG
		 * (TransactionIdDidCommit) before reporting COMMITTED.  A pre-commit
		 * stamp left by an xact that then aborted thus does not become visible.
		 */
		if (cluster_tt_durable_lookup && cluster_node_id >= 0
			&& key->origin_node_id == (uint16)cluster_node_id && key->tt_slot_id >= 1
			&& key->tt_slot_id <= TT_SLOTS_PER_SEGMENT) {
			SCN durable_scn;

			if (cluster_tt_slot_durable_lookup(key->undo_segment_id,
											   cluster_tt_slot_id_to_offset(key->tt_slot_id),
											   key->local_xid, &durable_scn)
				&& TransactionIdDidCommit(key->local_xid)) {
				result->status = CLUSTER_TT_STATUS_COMMITTED;
				result->commit_scn = durable_scn;
				result->status_epoch = current_epoch;
				result->authoritative = true;
				return true;
			}
		}
		return false;
	}

	result->status = e->status;
	result->commit_scn = e->commit_scn;
	result->status_epoch = e->status_epoch;
	result->authoritative = true;
	/*
	 * PGRAC (spec-3.5): copy parent_key chain metadata only for
	 * SUBCOMMITTED entries.  Bump dedicated SUBCOMMITTED hit counter.
	 */
	if (e->status == CLUSTER_TT_STATUS_SUBCOMMITTED && e->has_parent_key) {
		result->has_parent_key = true;
		result->parent_key = e->parent_key;
	}

	LWLockRelease(ClusterTTStatusLock);
	pg_atomic_fetch_add_u64(&ClusterTTStatusState->lookup_hit_count, 1);
	if (result->status == CLUSTER_TT_STATUS_SUBCOMMITTED)
		pg_atomic_fetch_add_u64(&ClusterTTStatusState->subcommitted_lookup_hit_count, 1);
	return true;
}

bool
cluster_tt_status_install_local(const ClusterTTStatusKey *key, ClusterTTStatus status,
								SCN commit_scn)
{
	ClusterTTOverlayEntry *e;
	bool found;

	Assert(key != NULL);

	if (!cluster_enabled || ClusterTTStatusHTAB == NULL)
		return false;

	LWLockAcquire(ClusterTTStatusLock, LW_EXCLUSIVE);

	e = (ClusterTTOverlayEntry *)hash_search(ClusterTTStatusHTAB, key, HASH_ENTER_NULL, &found);
	if (e == NULL) {
		/*
		 * HTAB full and no eviction implemented in foundation spec.  The
		 * overlay is best-effort: a miss returns UNKNOWN per HC181 and newer
		 * durable-TT paths can still resolve committed rows.  Do not emit a
		 * client WARNING per commit after the first capacity miss; that floods
		 * pgbench / OLTP clients and turns observability into write-path cost.
		 */
		LWLockRelease(ClusterTTStatusLock);
		note_overlay_full("install");
		return false;
	}

	e->status = status;
	e->commit_scn = commit_scn;
	e->status_epoch = key->cluster_epoch;
	e->install_ts = GetCurrentTimestamp();
	/*
	 * PGRAC (spec-3.5): install_local NEVER carries parent_key — it is
	 * reserved for install_subcommitted.  Clear here defensively.
	 */
	e->has_parent_key = false;
	memset(&e->parent_key, 0, sizeof(e->parent_key));

	LWLockRelease(ClusterTTStatusLock);

	pg_atomic_fetch_add_u64(&ClusterTTStatusState->install_count, 1);
	return true;
}

/*
 * cluster_tt_status_install_subcommitted (spec-3.5 D2 NEW)
 *
 *	  Installs SUBCOMMITTED status with parent_key chain pointer.
 *	  Used by spec-3.5 D7 xact.c CommitSubTransaction hook.  Caller
 *	  MUST first ensure parent_key has its own overlay binding via
 *	  cluster_subtrans_ensure_parent_binding().
 */
bool
cluster_tt_status_install_subcommitted(const ClusterTTStatusKey *child_key,
									   const ClusterTTStatusKey *parent_key)
{
	ClusterTTOverlayEntry *e;
	bool found;

	Assert(child_key != NULL);
	Assert(parent_key != NULL);

	if (!cluster_enabled || ClusterTTStatusHTAB == NULL)
		return false;

	LWLockAcquire(ClusterTTStatusLock, LW_EXCLUSIVE);

	e = (ClusterTTOverlayEntry *)hash_search(ClusterTTStatusHTAB, child_key, HASH_ENTER_NULL,
											 &found);
	if (e == NULL) {
		LWLockRelease(ClusterTTStatusLock);
		note_overlay_full("subcommitted install");
		return false;
	}

	e->status = CLUSTER_TT_STATUS_SUBCOMMITTED;
	e->commit_scn = InvalidScn; /* subxact not yet finalized */
	e->status_epoch = child_key->cluster_epoch;
	e->install_ts = GetCurrentTimestamp();
	e->has_parent_key = true;
	e->parent_key = *parent_key;

	LWLockRelease(ClusterTTStatusLock);

	pg_atomic_fetch_add_u64(&ClusterTTStatusState->install_count, 1);
	pg_atomic_fetch_add_u64(&ClusterTTStatusState->subcommitted_install_count, 1);
	return true;
}

/*
 * cluster_tt_status_delete_exact (spec-3.4c D6 / F4)
 *
 *	  Per-key delete companion of install_local.  Used by D5b
 *	  cluster_test_clear_visibility_injects() to remove one overlay
 *	  entry without flush_all()'s blast radius.  F4: must not fake-
 *	  clear by writing ABORTED — semantic conflict with the real
 *	  TT status state machine.
 */
bool
cluster_tt_status_delete_exact(const ClusterTTStatusKey *key)
{
	bool found;

	Assert(key != NULL);

	if (!cluster_enabled || ClusterTTStatusHTAB == NULL)
		return false;

	LWLockAcquire(ClusterTTStatusLock, LW_EXCLUSIVE);
	(void)hash_search(ClusterTTStatusHTAB, key, HASH_REMOVE, &found);
	LWLockRelease(ClusterTTStatusLock);

	if (found)
		pg_atomic_fetch_add_u64(&ClusterTTStatusState->evict_count, 1);

	return found;
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


/*
 * cluster_tt_status_flush_all_at_activation
 *
 *	spec-3.4b D8 / Q4 HC (L191): wipe the overlay HTAB unconditionally,
 *	called from postmaster shmem init AFTER the shmem region is set up.
 *	This is the **code-enforced** guarantee that no spec-3.1 / spec-3.2 /
 *	spec-3.3 / spec-3.4a provisional overlay entries can survive into the
 *	spec-3.4b real-allocator era; runbook-based reconfig flush remains as
 *	a fallback at cluster_reconfig.c:571 but is NOT the only correctness
 *	mechanism.
 *
 *	Hard contract (L191):
 *	  "Real TT allocator activation must clear all provisional TT-status
 *	   overlay entries before any production visibility lookup can
 *	   consume real TT keys.  Reconfig-triggered flush is a fallback, not
 *	   the only correctness mechanism."
 *
 *	Safe to call before cluster_enabled is set: the function tolerates a
 *	NULL HTAB pointer (returns silently).  Calling twice is a no-op.
 *
 *	NOT a postmaster-only callback per CLAUDE.md rule 16: the function
 *	runs during ShmemInitStruct of postmaster, where the HTAB pointer is
 *	first set; later backend forks inherit a clean HTAB without re-flush.
 *	Under EXEC_BACKEND (Windows) each backend re-runs the init path; the
 *	flush stays correct because it is idempotent (find nothing, remove
 *	nothing).
 */
void
cluster_tt_status_flush_all_at_activation(void)
{
	HASH_SEQ_STATUS hseq;
	const ClusterTTOverlayEntry *e;
	uint64 removed = 0;

	if (ClusterTTStatusHTAB == NULL)
		return; /* shmem not initialised yet; nothing to flush */

	LWLockAcquire(ClusterTTStatusLock, LW_EXCLUSIVE);

	hash_seq_init(&hseq, ClusterTTStatusHTAB);
	while ((e = (ClusterTTOverlayEntry *)hash_seq_search(&hseq)) != NULL) {
		hash_search(ClusterTTStatusHTAB, &e->key, HASH_REMOVE, NULL);
		removed++;
	}

	if (ClusterTTStatusState != NULL) {
		pg_atomic_fetch_add_u64(&ClusterTTStatusState->generation, 1);
		pg_atomic_fetch_add_u64(&ClusterTTStatusState->flush_count, 1);
		pg_atomic_fetch_add_u64(&ClusterTTStatusState->evict_count, removed);
	}

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
/* PGRAC (spec-3.5): SUBCOMMITTED counters. */
CLUSTER_TT_STATUS_COUNTER_GETTER(subcommitted_install_count)
CLUSTER_TT_STATUS_COUNTER_GETTER(subcommitted_lookup_hit_count)
CLUSTER_TT_STATUS_COUNTER_GETTER(parent_chain_follow_count)

void
cluster_tt_status_bump_parent_chain_follow(void)
{
	if (ClusterTTStatusState != NULL)
		pg_atomic_fetch_add_u64(&ClusterTTStatusState->parent_chain_follow_count, 1);
}

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
		/* PGRAC (spec-3.5): parent_key sentinel in disable-cluster build. */
		result->has_parent_key = false;
		memset(&result->parent_key, 0, sizeof(result->parent_key));
	}
	(void)key;
	return false;
}

bool
cluster_tt_status_install_local(const ClusterTTStatusKey *key, ClusterTTStatus status,
								SCN commit_scn)
{
	(void)key;
	(void)status;
	(void)commit_scn;
	return false;
}

bool
cluster_tt_status_install_subcommitted(const ClusterTTStatusKey *child_key,
									   const ClusterTTStatusKey *parent_key)
{
	(void)child_key;
	(void)parent_key;
	return false;
}

bool
cluster_tt_status_delete_exact(const ClusterTTStatusKey *key)
{
	(void)key;
	return false;
}

void
cluster_tt_status_flush_all(uint32 new_epoch)
{
	(void)new_epoch;
}

void
cluster_tt_status_flush_all_at_activation(void)
{}

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
/* PGRAC (spec-3.5) */
CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(subcommitted_install_count)
CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(subcommitted_lookup_hit_count)
CLUSTER_TT_STATUS_COUNTER_GETTER_STUB(parent_chain_follow_count)

void
cluster_tt_status_bump_parent_chain_follow(void)
{}

#endif /* USE_PGRAC_CLUSTER */
