/*-------------------------------------------------------------------------
 *
 * cluster_undo_gcs_stat.c
 *	  Shared-undo block GCS data-plane observability (spec-5.22b D2-6).
 *
 *	  Shmem counter region for the owner-as-master undo grant plane
 *	  implemented in cluster_undo_gcs_grant.c.  Kept in a separate translation
 *	  unit so the pure decision core (cluster_undo_gcs.c) links standalone into
 *	  cluster_unit without dragging in shmem symbols: the runtime primitives in
 *	  cluster_undo_gcs_grant.c (already a backend-only object) call the extern
 *	  bump hooks below, and dump_undo reads them lock-free via the accessors.
 *
 *	  Six atomic counters (no LWLock), surfaced under category='undo' by
 *	  cluster_debug.c dump_undo: grant_shared / grant_exclusive / ship_bytes /
 *	  invalidate_notify / remaster_deny / local_fast_path.
 *
 *	  The counters are register-ahead of a live consumer: the grant primitives
 *	  they observe have no live caller until the D6 consumer wiring lands (the
 *	  same skeleton-ahead posture as D2-3/D2-4), so at rest every counter reads
 *	  0 -- but the observability surface (shmem region + dump keys) is present
 *	  and queryable now.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_gcs_stat.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.22b-undo-block-gcs-integration.md (D2-6)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "port/atomics.h"
#include "storage/shmem.h"

#include "cluster/cluster_shmem.h"
#include "cluster/cluster_undo_gcs.h"

/*
 * ClusterUndoGcsShared -- per-instance shmem counters (spec-5.22b D2-6).
 *
 *	Six atomic counters, no LWLock (region lwlock_count = 0): bumped with
 *	pg_atomic_fetch_add_u64 from the granting / invalidating backend, read
 *	lock-free by dump_undo.
 */
typedef struct ClusterUndoGcsShared {
	pg_atomic_uint64 grant_shared_count;	  /* reader S-grant admitted (D2-3) */
	pg_atomic_uint64 grant_exclusive_count;	  /* writer/cleaner X-grant taken (D2-4) */
	pg_atomic_uint64 ship_bytes;			  /* bytes admitted on a coherent S view */
	pg_atomic_uint64 invalidate_notify_count; /* peers sent a PI_DISCARD (D2-4) */
	pg_atomic_uint64 remaster_deny_count;	  /* serve-gate RESOURCE_RECOVERING deny (D2-5) */
	pg_atomic_uint64 local_fast_path_count;	  /* master==self routing taken (no network) */
} ClusterUndoGcsShared;

#ifdef USE_PGRAC_CLUSTER

static ClusterUndoGcsShared *UndoGcsShared = NULL;

/* ------------------------------------------------------------ */
/* shmem region                                                 */
/* ------------------------------------------------------------ */

Size
cluster_undo_gcs_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterUndoGcsShared));
}

void
cluster_undo_gcs_shmem_init(void)
{
	bool found;

	UndoGcsShared = ShmemInitStruct("ClusterUndoGcsShared", cluster_undo_gcs_shmem_size(), &found);

	if (!found) {
		pg_atomic_init_u64(&UndoGcsShared->grant_shared_count, 0);
		pg_atomic_init_u64(&UndoGcsShared->grant_exclusive_count, 0);
		pg_atomic_init_u64(&UndoGcsShared->ship_bytes, 0);
		pg_atomic_init_u64(&UndoGcsShared->invalidate_notify_count, 0);
		pg_atomic_init_u64(&UndoGcsShared->remaster_deny_count, 0);
		pg_atomic_init_u64(&UndoGcsShared->local_fast_path_count, 0);
	}
}

static const ClusterShmemRegion cluster_undo_gcs_region = {
	.name = "pgrac cluster undo gcs",
	.size_fn = cluster_undo_gcs_shmem_size,
	.init_fn = cluster_undo_gcs_shmem_init,
	.lwlock_count = 0, /* atomic counters only; no LWLock */
	.owner_subsys = "cluster_undo_gcs",
	.reserved_flags = 0,
};

void
cluster_undo_gcs_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_undo_gcs_region);
}

/* ------------------------------------------------------------ */
/* counter bump hooks (called from cluster_undo_gcs_grant.c)     */
/* ------------------------------------------------------------ */

void
cluster_undo_gcs_count_grant_shared(uint32 bytes)
{
	if (UndoGcsShared == NULL)
		return;
	pg_atomic_fetch_add_u64(&UndoGcsShared->grant_shared_count, 1);
	pg_atomic_fetch_add_u64(&UndoGcsShared->ship_bytes, (uint64)bytes);
}

void
cluster_undo_gcs_count_grant_exclusive(void)
{
	if (UndoGcsShared != NULL)
		pg_atomic_fetch_add_u64(&UndoGcsShared->grant_exclusive_count, 1);
}

void
cluster_undo_gcs_count_invalidate_notify(int peers)
{
	if (UndoGcsShared != NULL && peers > 0)
		pg_atomic_fetch_add_u64(&UndoGcsShared->invalidate_notify_count, (uint64)peers);
}

void
cluster_undo_gcs_count_remaster_deny(void)
{
	if (UndoGcsShared != NULL)
		pg_atomic_fetch_add_u64(&UndoGcsShared->remaster_deny_count, 1);
}

void
cluster_undo_gcs_count_local_fast_path(void)
{
	if (UndoGcsShared != NULL)
		pg_atomic_fetch_add_u64(&UndoGcsShared->local_fast_path_count, 1);
}

/* ------------------------------------------------------------ */
/* lock-free read accessors (dump_undo)                          */
/* ------------------------------------------------------------ */

uint64
cluster_undo_gcs_grant_shared_count(void)
{
	return UndoGcsShared != NULL ? pg_atomic_read_u64(&UndoGcsShared->grant_shared_count) : 0;
}

uint64
cluster_undo_gcs_grant_exclusive_count(void)
{
	return UndoGcsShared != NULL ? pg_atomic_read_u64(&UndoGcsShared->grant_exclusive_count) : 0;
}

uint64
cluster_undo_gcs_ship_bytes(void)
{
	return UndoGcsShared != NULL ? pg_atomic_read_u64(&UndoGcsShared->ship_bytes) : 0;
}

uint64
cluster_undo_gcs_invalidate_notify_count(void)
{
	return UndoGcsShared != NULL ? pg_atomic_read_u64(&UndoGcsShared->invalidate_notify_count) : 0;
}

uint64
cluster_undo_gcs_remaster_deny_count(void)
{
	return UndoGcsShared != NULL ? pg_atomic_read_u64(&UndoGcsShared->remaster_deny_count) : 0;
}

uint64
cluster_undo_gcs_local_fast_path_count(void)
{
	return UndoGcsShared != NULL ? pg_atomic_read_u64(&UndoGcsShared->local_fast_path_count) : 0;
}

#endif /* USE_PGRAC_CLUSTER */
