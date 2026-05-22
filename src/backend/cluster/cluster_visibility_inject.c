/*-------------------------------------------------------------------------
 *
 * cluster_visibility_inject.c
 *	  pgrac test-only visibility cluster path inject mechanism.
 *
 *	  spec-3.2 D5b (NEW;v0.3 N3 driver).
 *
 *	  ENABLE_INJECTION conditional:  production binary (no
 *	  --enable-injection-points configure flag) gets a stub body:
 *	  lookup helper returns false, no GUC is registered, and SQL UDFs
 *	  raise FEATURE_NOT_SUPPORTED.  See header file for full design
 *	  rationale.
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.2-mvcc-cluster-path-tt-status-wire.md (v1.0 FROZEN 2026-05-22)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_visibility_inject.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_visibility_inject.h"
#include "fmgr.h"

#ifdef ENABLE_INJECTION

#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_shmem.h"

#define CLUSTER_VISIBILITY_INJECT_CAPACITY 256

typedef struct ClusterVisibilityInjectEntry {
	TransactionId xid; /* HTAB key */
	ClusterUndoTTSlotRef ref;
} ClusterVisibilityInjectEntry;

static HTAB *ClusterVisibilityInjectHTAB = NULL;
static LWLock *ClusterVisibilityInjectLock = NULL;

/* ------------------------------------------------------------ */
/* shmem layout                                                 */
/* ------------------------------------------------------------ */

Size
cluster_visibility_inject_shmem_size(void)
{
	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return 0;
	return hash_estimate_size(CLUSTER_VISIBILITY_INJECT_CAPACITY,
							  sizeof(ClusterVisibilityInjectEntry))
		   + MAXALIGN(sizeof(LWLockPadded));
}

void
cluster_visibility_inject_shmem_init(void)
{
	HASHCTL info;
	LWLockPadded *lockblock;
	bool found;

	if (IsBootstrapProcessingMode() || !cluster_enabled || cluster_node_id < 0)
		return;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(TransactionId);
	info.entrysize = sizeof(ClusterVisibilityInjectEntry);
	info.num_partitions = 1;
	ClusterVisibilityInjectHTAB
		= ShmemInitHash("ClusterVisibilityInject", CLUSTER_VISIBILITY_INJECT_CAPACITY,
						CLUSTER_VISIBILITY_INJECT_CAPACITY, &info, HASH_ELEM | HASH_BLOBS);

	lockblock = (LWLockPadded *)ShmemInitStruct("ClusterVisibilityInjectLock",
												MAXALIGN(sizeof(LWLockPadded)), &found);
	if (!found)
		LWLockInitialize(&lockblock->lock, LWTRANCHE_CLUSTER_TT_STATUS);
	ClusterVisibilityInjectLock = &lockblock->lock;
}

static const ClusterShmemRegion cluster_visibility_inject_region = {
	.name = "pgrac cluster visibility inject",
	.size_fn = cluster_visibility_inject_shmem_size,
	.init_fn = cluster_visibility_inject_shmem_init,
	.lwlock_count = 1,
	.owner_subsys = "cluster_visibility_inject",
	.reserved_flags = 0,
};

void
cluster_visibility_inject_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_visibility_inject_region);
}

/* ------------------------------------------------------------ */
/* lookup helper used by D5 fork                                */
/* ------------------------------------------------------------ */

bool
cluster_test_lookup_visibility_inject(TransactionId xid, ClusterUndoTTSlotRef *ref)
{
	ClusterVisibilityInjectEntry *e;
	bool hit = false;

	if (!cluster_test_force_visibility_cluster_path || ClusterVisibilityInjectHTAB == NULL
		|| ref == NULL)
		return false;

	LWLockAcquire(ClusterVisibilityInjectLock, LW_SHARED);
	e = (ClusterVisibilityInjectEntry *)hash_search(ClusterVisibilityInjectHTAB, &xid, HASH_FIND,
													NULL);
	if (e != NULL) {
		*ref = e->ref;
		hit = true;
	}
	LWLockRelease(ClusterVisibilityInjectLock);
	return hit;
}

/* ------------------------------------------------------------ */
/* SQL UDF (superuser only)                                     */
/* ------------------------------------------------------------ */

PG_FUNCTION_INFO_V1(cluster_test_inject_visibility_tt_ref);
PG_FUNCTION_INFO_V1(cluster_test_clear_visibility_injects);

Datum
cluster_test_inject_visibility_tt_ref(PG_FUNCTION_ARGS)
{
	TransactionId xid;
	uint16 origin;
	uint16 segment;
	uint32 slot;
	uint32 epoch;
	ClusterVisibilityInjectEntry *e;
	bool found;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to use cluster_test_inject_visibility_tt_ref")));

	if (ClusterVisibilityInjectHTAB == NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster visibility inject shmem not initialized")));

	xid = (TransactionId)PG_GETARG_UINT32(0);
	origin = (uint16)PG_GETARG_INT32(1);
	segment = (uint16)PG_GETARG_INT32(2);
	slot = (uint32)PG_GETARG_INT32(3);
	epoch = (uint32)PG_GETARG_INT32(4);

	LWLockAcquire(ClusterVisibilityInjectLock, LW_EXCLUSIVE);
	e = (ClusterVisibilityInjectEntry *)hash_search(ClusterVisibilityInjectHTAB, &xid,
													HASH_ENTER_NULL, &found);
	if (e == NULL) {
		LWLockRelease(ClusterVisibilityInjectLock);
		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("cluster visibility inject table full")));
	}

	memset(&e->ref, 0, sizeof(e->ref));
	e->ref.origin_node_id = origin;
	e->ref.undo_segment_id = segment;
	e->ref.tt_slot_id = slot;
	e->ref.cluster_epoch = epoch;
	e->ref.local_xid = xid;
	e->ref.has_cached_status = false;
	LWLockRelease(ClusterVisibilityInjectLock);

	PG_RETURN_BOOL(true);
}

Datum
cluster_test_clear_visibility_injects(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hseq;
	ClusterVisibilityInjectEntry *e;
	int removed = 0;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser to clear cluster visibility injects")));

	if (ClusterVisibilityInjectHTAB == NULL)
		PG_RETURN_INT32(0);

	LWLockAcquire(ClusterVisibilityInjectLock, LW_EXCLUSIVE);
	hash_seq_init(&hseq, ClusterVisibilityInjectHTAB);
	while ((e = (ClusterVisibilityInjectEntry *)hash_seq_search(&hseq)) != NULL) {
		hash_search(ClusterVisibilityInjectHTAB, &e->xid, HASH_REMOVE, NULL);
		removed++;
	}
	LWLockRelease(ClusterVisibilityInjectLock);

	PG_RETURN_INT32(removed);
}

#else /* !ENABLE_INJECTION */

Size
cluster_visibility_inject_shmem_size(void)
{
	return 0;
}
void
cluster_visibility_inject_shmem_init(void)
{}
void
cluster_visibility_inject_shmem_register(void)
{}
bool
cluster_test_lookup_visibility_inject(TransactionId xid, ClusterUndoTTSlotRef *ref)
{
	(void)xid;
	(void)ref;
	return false;
}

PG_FUNCTION_INFO_V1(cluster_test_inject_visibility_tt_ref);
PG_FUNCTION_INFO_V1(cluster_test_clear_visibility_injects);

Datum
cluster_test_inject_visibility_tt_ref(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster visibility inject support is not enabled"),
					errhint("Rebuild with --enable-injection-points to use "
							"cluster_test_inject_visibility_tt_ref().")));
	PG_RETURN_BOOL(false);
}

Datum
cluster_test_clear_visibility_injects(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster visibility inject support is not enabled"),
					errhint("Rebuild with --enable-injection-points to use "
							"cluster_test_clear_visibility_injects().")));
	PG_RETURN_INT32(0);
}

#endif /* ENABLE_INJECTION */
