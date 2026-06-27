/*-------------------------------------------------------------------------
 *
 * cluster_cr_tuple_stat.c
 *	  pgrac tuple-level CR fast-path outcome counters (spec-5.54 D5).
 *
 *	  An INDEPENDENT, small, fixed-size shared-memory region holding instance-
 *	  wide atomic counters -- one per ClusterCRTupleOutcome (the VERDICT success
 *	  plus each FALLBACK reason).  Deliberately SEPARATE from the spec-5.51
 *	  ClusterCRShared struct (and from the spec-5.52 admission region) so the held
 *	  5.51 substrate layout is not changed: this is purely additive observability
 *	  for pg_cluster_state (spec-5.54 P2#1 / F0-26, mirroring spec-5.52 D9).
 *
 *	  The D3 gate bumps the counter for each fast-path attempt's outcome; the dump
 *	  reads them lock-free.  Advisory only -- corruption or staleness affects
 *	  observability, never correctness (INV-T-counters never feed a verdict).
 *	  Always allocated (a handful of uint64s), so the region count is
 *	  deterministic regardless of GUC/pool state.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.54-tuple-level-cr-verdict-only.md (FROZEN v1.0)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_cr_tuple_stat.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_tuple.h"
#include "cluster/cluster_shmem.h" /* ClusterShmemRegion, register */
#include "port/atomics.h"
#include "storage/shmem.h" /* ShmemInitStruct */

/*
 * ClusterCRTupleStat -- one atomic counter per fast-path outcome.  Indexed by
 * ClusterCRTupleOutcome; CR_TUPLE_OUTCOME__COUNT is the array bound.
 */
typedef struct ClusterCRTupleStat {
	pg_atomic_uint64 outcome[CR_TUPLE_OUTCOME__COUNT];
} ClusterCRTupleStat;

static ClusterCRTupleStat *CRTupleStat = NULL;

Size
cluster_cr_tuple_stat_shmem_size(void)
{
	return MAXALIGN(sizeof(ClusterCRTupleStat));
}

void
cluster_cr_tuple_stat_shmem_init(void)
{
	bool found;

	CRTupleStat = (ClusterCRTupleStat *)ShmemInitStruct("ClusterCRTupleStat",
														cluster_cr_tuple_stat_shmem_size(), &found);
	if (!found) {
		int i;

		for (i = 0; i < CR_TUPLE_OUTCOME__COUNT; i++)
			pg_atomic_init_u64(&CRTupleStat->outcome[i], 0);
	}
}

static const ClusterShmemRegion cluster_cr_tuple_stat_region = {
	.name = "pgrac cluster cr tuple stats",
	.size_fn = cluster_cr_tuple_stat_shmem_size,
	.init_fn = cluster_cr_tuple_stat_shmem_init,
	.lwlock_count = 0, /* lock-free atomic counters */
	.owner_subsys = "cluster_cr_tuple",
	.reserved_flags = 0,
};

void
cluster_cr_tuple_stat_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_cr_tuple_stat_region);
}

void
cluster_cr_tuple_stat_bump(ClusterCRTupleOutcome outcome)
{
	if (CRTupleStat == NULL || outcome < 0 || outcome >= CR_TUPLE_OUTCOME__COUNT)
		return;
	pg_atomic_fetch_add_u64(&CRTupleStat->outcome[outcome], 1);
}

uint64
cluster_cr_tuple_stat_count(ClusterCRTupleOutcome outcome)
{
	if (CRTupleStat == NULL || outcome < 0 || outcome >= CR_TUPLE_OUTCOME__COUNT)
		return 0;
	return pg_atomic_read_u64(&CRTupleStat->outcome[outcome]);
}
