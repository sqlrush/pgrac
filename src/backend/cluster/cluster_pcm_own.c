/*-------------------------------------------------------------------------
 *
 * cluster_pcm_own.c
 *	  pgrac per-buffer node-local ownership generation + flags (shmem array).
 *
 *	  See cluster_pcm_own.h for the design.  This file owns the shmem array
 *	  allocation (NBuffers entries, indexed by buf_id) and its registration
 *	  with the cluster shmem region registry.  The coherent transition/read
 *	  helpers live in bufmgr.c (they need BufferDesc + the header spinlock).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_pcm_own.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-4.7a-hold-until-revoked.md (ownership-generation wave).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "miscadmin.h"
#include "storage/shmem.h"

#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_shmem.h"

ClusterPcmOwnEntry *ClusterPcmOwnArray = NULL;

Size
cluster_pcm_own_shmem_size(void)
{
	return mul_size((Size) NBuffers, sizeof(ClusterPcmOwnEntry));
}

void
cluster_pcm_own_shmem_init(void)
{
	bool found;

	ClusterPcmOwnArray
		= (ClusterPcmOwnEntry *) ShmemInitStruct("pgrac cluster pcm ownership",
												 cluster_pcm_own_shmem_size(), &found);
	if (!found) {
		int i;

		for (i = 0; i < NBuffers; i++) {
			pg_atomic_init_u64(&ClusterPcmOwnArray[i].generation, 0);
			pg_atomic_init_u32(&ClusterPcmOwnArray[i].flags, 0);
			ClusterPcmOwnArray[i]._pad = 0;
		}
	}
}

static const ClusterShmemRegion cluster_pcm_own_region = {
	.name = "pgrac cluster pcm ownership",
	.size_fn = cluster_pcm_own_shmem_size,
	.init_fn = cluster_pcm_own_shmem_init,
	.lwlock_count = 0, /* atomics only; the buffer header spinlock serializes the triple */
	.owner_subsys = "cluster_pcm_own",
	.reserved_flags = 0,
};

void
cluster_pcm_own_shmem_register(void)
{
	cluster_shmem_register_region(&cluster_pcm_own_region);
}

#endif							/* USE_PGRAC_CLUSTER */
