/*-------------------------------------------------------------------------
 *
 * cluster_ir.c
 *	  IR (instance-recovery owner) cross-node enqueue lock -- PURE layer
 *	  (spec-5.7 §3.4 / D8).  Just the resource-id encoder and the bootstrap-phase
 *	  predicate, standalone-linkable so the cluster_unit test links them directly.
 *	  The backend (shmem counters, GES acquire/release, recovery hook) lives in
 *	  cluster_ir_lock.c.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ir.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D8, §3.4)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_ir.h"


/*
 * cluster_ir_resid_encode -- build the IR resource id for (dead_node, episode).
 * field1 = dead_node_id; field2/field3 = the low/high 32 bits of the 64-bit
 * episode epoch (so a new reconfig episode is a distinct resid -- IR-M2);
 * field4 = 0.  Both survivors recovering the same dead node in the same episode
 * hash to the same resid and compete for IR(X).
 */
void
cluster_ir_resid_encode(int32 dead_node_id, uint64 episode_epoch, ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = (uint32)dead_node_id;
	dst->field2 = (uint32)(episode_epoch & 0xFFFFFFFFU);
	dst->field3 = (uint32)(episode_epoch >> 32);
	dst->field4 = 0;
	dst->type = CLUSTER_IR_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

/*
 * cluster_ir_bootstrap_ready -- IR-M5 precondition: the launch episode epoch must
 * be the current accepted epoch (and a real episode).  A mismatch means the
 * reconfig advanced past this worker -> not ready (defer; the L235 abort owns the
 * runtime path).  Pure so the unit test can drive it without the GES.
 */
bool
cluster_ir_bootstrap_ready(uint64 episode_epoch, uint64 accepted_epoch)
{
	return episode_epoch != 0 && episode_epoch == accepted_epoch;
}
