/*-------------------------------------------------------------------------
 *
 * cluster_dl.c
 *	  DL (bulk-load lease) cross-node coordination lock -- PURE layer
 *	  (spec-5.7 §3.2 / D4).  Just the resource-id encoder, standalone-linkable
 *	  so the cluster_unit test links it directly.  The backend (shmem counters,
 *	  GES acquire/release, BulkInsertState lease wrappers) lives in
 *	  cluster_dl_lock.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_dl.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D4, §3.2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_dl.h"


/*
 * cluster_dl_resid_encode -- build the DL resource id for a relation.  DL is
 * per-RELATION (field4 = 0, no fork): two nodes bulk-loading the same relation
 * hash to the same resid and serialise on DL(X).  relNumber is the relfilenode
 * (ABA defence).
 */
void
cluster_dl_resid_encode(RelFileLocator rloc, ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = (uint32)rloc.dbOid;
	dst->field2 = (uint32)rloc.relNumber;
	dst->field3 = (uint32)rloc.spcOid;
	dst->field4 = 0;
	dst->type = CLUSTER_DL_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}
