/*-------------------------------------------------------------------------
 *
 * cluster_grd_pending.c
 *	  Pending GES request table — spec-2.16 D3 (skeleton phase).
 *
 *	  Skeleton (Step 1):  shmem region stub + 5 API stubs returning
 *	  ERRCODE_FEATURE_NOT_SUPPORTED / no-op per规则 8.  Step 2-3 真激活:
 *	    - Step 2 wires the HTAB (4-tuple keyed dynahash + GUC sizing)
 *	    - Step 3 wires register / signal / release / lookup paths
 *
 *	  See cluster_grd_pending.h for protocol contract.
 *
 *	  Spec: spec-2.16-cross-node-grant-convert-mvp.md (DRAFT v0.1)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_grd_pending.c
 *
 * NOTES
 *	  Skeleton phase: 0 caller / 0 mutation. cluster_unit Step 6
 *	  tests will assert FEATURE_NOT_SUPPORTED until Step 3 lands.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_grd_pending.h"
#include "cluster/cluster_shmem.h"
#include "miscadmin.h"
#include "storage/shmem.h"
#include "utils/elog.h"


/* ============================================================
 * Shmem region — Step 1 skeleton (0 bytes, no real HTAB).
 *
 *   Step 2 D4 amend:  size_fn computes hash_estimate_size for the
 *   pending HTAB with GUC-derived max_size = cluster.ges_pending_max
 *   (NEW GUC Step 5 D12, default = NBackends * 4 per spec-2.16 Q3).
 * ============================================================ */

Size
cluster_grd_pending_shmem_size(void)
{
	return 0; /* skeleton — Step 2 D4 真激活 */
}

void
cluster_grd_pending_shmem_init(void)
{
	/* skeleton — Step 2 D4 allocates HTAB + counter slot */
}

static const ClusterShmemRegion cluster_grd_pending_region = {
	.name = "pgrac cluster grd pending",
	.size_fn = cluster_grd_pending_shmem_size,
	.init_fn = cluster_grd_pending_shmem_init,
	.lwlock_count = 0, /* Step 2:  partition LWLock count = NUM_LOCK_PARTITIONS */
	.owner_subsys = "cluster_grd_pending",
	.reserved_flags = 0,
};

void
cluster_grd_pending_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_grd_pending_region);
}


/* ============================================================
 * 5 API stubs — Step 3 D6 真激活.
 *
 *   规则 8:  ERRCODE_FEATURE_NOT_SUPPORTED for write paths;
 *   read paths (lookup_state / count) return safe default
 *   (no FEATURE_NOT_SUPPORTED ereport — caller cluster_debug
 *   emit_row + observability must not crash on Step 1 skeleton).
 * ============================================================ */

void
cluster_grd_pending_register(const ClusterGrdPendingKey *key pg_attribute_unused())
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster_grd_pending_register not implemented in Step 1"),
			 errhint("spec-2.16 Step 3 D6 activates pending table register")));
}

void
cluster_grd_pending_signal(const ClusterGrdPendingKey *key pg_attribute_unused(),
						   ClusterGrdPendingState verdict pg_attribute_unused())
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster_grd_pending_signal not implemented in Step 1"),
			 errhint("spec-2.16 Step 3 D6 activates LMON reply CAS + SetLatch")));
}

void
cluster_grd_pending_release(const ClusterGrdPendingKey *key pg_attribute_unused())
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cluster_grd_pending_release not implemented in Step 1"),
			 errhint("spec-2.16 Step 3 D6 activates HASH_REMOVE on post-wait")));
}

ClusterGrdPendingState
cluster_grd_pending_lookup_state(const ClusterGrdPendingKey *key pg_attribute_unused())
{
	/* skeleton:  safe default ENQUEUED;  Step 3 D6 真 lookup */
	return CLUSTER_GRD_PENDING_ENQUEUED;
}

uint64
cluster_grd_pending_count(void)
{
	return 0; /* skeleton — Step 2 D4 wires atomic count */
}
