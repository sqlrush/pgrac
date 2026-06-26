/*-------------------------------------------------------------------------
 *
 * cluster_cr_admit_stat.c
 *	  pgrac shared CR pool admission reason counters (spec-5.52 D9).
 *
 *	  An INDEPENDENT, small, fixed-size shared-memory region holding instance-
 *	  wide atomic counters -- one per ClusterCRAdmitReason (admit + each reject
 *	  reason).  Deliberately SEPARATE from the spec-5.51 ClusterCRShared struct
 *	  so the held 5.51 substrate layout is not changed: this is purely additive
 *	  observability for pg_cluster_state.
 *
 *	  The D2 gate bumps the counter for the last admit() decision; the dump
 *	  reads them lock-free.  Advisory only -- corruption or staleness affects
 *	  observability, never correctness (INV-A3).  Always allocated (a handful of
 *	  uint64s), so the region count is deterministic regardless of pool state.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.52-cr-cache-admission-policy.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_admit_stat.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_admit.h"
#include "cluster/cluster_shmem.h" /* ClusterShmemRegion, register */
#include "port/atomics.h"
#include "storage/shmem.h" /* ShmemInitStruct */

/*
 * ClusterCRAdmitStat -- one atomic counter per admission reason.  Indexed by
 * ClusterCRAdmitReason; CR_ADMIT_REASON__COUNT is the array bound.
 */
typedef struct ClusterCRAdmitStat {
	pg_atomic_uint64 reason[CR_ADMIT_REASON__COUNT];
} ClusterCRAdmitStat;

static ClusterCRAdmitStat *CRAdmitStat = NULL;

Size
cluster_cr_admit_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCRAdmitStat));
}

void
cluster_cr_admit_shmem_init(void)
{
	bool found;

	CRAdmitStat = (ClusterCRAdmitStat *)ShmemInitStruct("ClusterCRAdmitStat",
														cluster_cr_admit_shmem_size(), &found);
	if (!found) {
		int i;

		for (i = 0; i < CR_ADMIT_REASON__COUNT; i++)
			pg_atomic_init_u64(&CRAdmitStat->reason[i], 0);
	}
}

static const ClusterShmemRegion cluster_cr_admit_region = {
	.name = "pgrac cluster cr admit stats",
	.size_fn = cluster_cr_admit_shmem_size,
	.init_fn = cluster_cr_admit_shmem_init,
	.lwlock_count = 0, /* lock-free atomic counters */
	.owner_subsys = "cluster_cr_admit",
	.reserved_flags = 0,
};

void
cluster_cr_admit_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_admit_region);
}

void
cluster_cr_admit_stat_bump(ClusterCRAdmitReason reason)
{
	if (CRAdmitStat == NULL || reason < 0 || reason >= CR_ADMIT_REASON__COUNT)
		return;
	pg_atomic_fetch_add_u64(&CRAdmitStat->reason[reason], 1);
}

uint64
cluster_cr_admit_stat_count(ClusterCRAdmitReason reason)
{
	if (CRAdmitStat == NULL || reason < 0 || reason >= CR_ADMIT_REASON__COUNT)
		return 0;
	return pg_atomic_read_u64(&CRAdmitStat->reason[reason]);
}
