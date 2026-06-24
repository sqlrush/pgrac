/*-------------------------------------------------------------------------
 *
 * cluster_ko.c
 *	  KO (object-reuse flush) cross-node barrier -- PURE layer (spec-5.7 §3.5 /
 *	  D6).  Just the resource-id encoder, standalone-linkable so the cluster_unit
 *	  test links it directly.  The backend (KO(X) GES lock, the apply-after-drop
 *	  flush fanout + ACK barrier, the peer-side drain, shmem counters) lives in
 *	  cluster_ko_lock.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ko.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D6, §3.5)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ko.h"


/*
 * cluster_ko_resid_encode -- build the KO resource id for a relfilenode.  KO is
 * per-RELFILENODE (field4 = 0, no fork): a DROP/TRUNCATE removes all forks of
 * the relfilenode, so the flush barrier and KO(X) serialise on the relfilenode
 * triple.  field2 = relNumber is the relfilenode (ABA defence).
 */
void
cluster_ko_resid_encode(RelFileLocator rloc, ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = (uint32)rloc.dbOid;
	dst->field2 = (uint32)rloc.relNumber;
	dst->field3 = (uint32)rloc.spcOid;
	dst->field4 = 0;
	dst->type = CLUSTER_KO_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}
