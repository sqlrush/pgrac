/*-------------------------------------------------------------------------
 *
 * cluster_hw.c
 *	  HW (relation extend) cluster block-number authority -- pure layer
 *	  (spec-5.7a D1).
 *
 *	  This file ships the backend-pure layer: the HW resource-id encoder, the
 *	  three-state extend classifier (HW-M7), and the master-side batch
 *	  allocator math (§3.1a M1b).  None of these touch elog / shmem / locks, so
 *	  the cluster_unit test links the object standalone.  The backend layer
 *	  (master shmem HWM table, IC HW_ALLOC handler, reservation WAL, HW(X)
 *	  acquire/release) lands with the D1 backend / D2 buffer-manager
 *	  integration.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_hw.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.7-misc-enqueue-classes.md (D1, §2.1 / §3.1 / §3.1a)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_class.h" /* RELPERSISTENCE_* */
#include "cluster/cluster_hw.h"

/*
 * cluster_hw_resid_encode -- build the HW resource id for a relation fork.
 *
 *	field2 is the relfilenode (RelFileNumber), so a DROP+CREATE of the same
 *	relation oid lands a distinct resource (ABA defence), mirroring the SQ
 *	generation field.  field4 is the fork so the main / fsm / vm extents of a
 *	relation extend under independent HW resources.
 */
void
cluster_hw_resid_encode(RelFileLocator rloc, ForkNumber fork, ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;

	dst->field1 = (uint32)rloc.dbOid;
	dst->field2 = (uint32)rloc.relNumber;
	dst->field3 = (uint32)rloc.spcOid;
	dst->field4 = (uint16)fork;
	dst->type = CLUSTER_HW_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

/*
 * cluster_hw_classify_persistence -- the three-state extend gate (HW-M7,
 * spec-5.7 amend #3).
 *
 *	The classifier is deliberately three-state, not a boolean: mapping
 *	"persistence != permanent" to a local extend would silently corrupt an
 *	unlogged relation in a multi-node cluster.  Classification depends only on
 *	the persistence kind and whether the cluster is multi-node -- NOT on
 *	RelationNeedsWAL / new-in-txn (amend #3): the authority owns every permanent
 *	shared relation's extents from block 0, so there is no seed and the
 *	minimal-WAL new-in-txn case is GLOBALIZE like any other permanent relation.
 */
ClusterHwClass
cluster_hw_classify_persistence(char relpersistence, bool multi_node)
{
	/* temp relations use PG-local buffers and are never cross-node extended. */
	if (relpersistence == RELPERSISTENCE_TEMP)
		return CLUSTER_HW_NATIVE_LOCAL;

	/*
	 * Unlogged relations are not WAL-logged, so the HWM cannot be rebuilt from
	 * the reservation WAL after a crash / remaster.  In a multi-node cluster
	 * there is neither cross-node coherence nor a WAL authority -> fail closed
	 * (the caller must ereport 53RA6, never silently fall back to a local
	 * FileSize extend).  On a single node there is no cross-node coherence
	 * problem, so a PG-native local extend is safe.
	 */
	if (relpersistence == RELPERSISTENCE_UNLOGGED)
		return multi_node ? CLUSTER_HW_FAIL_CLOSED : CLUSTER_HW_NATIVE_LOCAL;

	/*
	 * All permanent shared relations -- INCLUDING new-in-txn / minimal-WAL --
	 * are owned by the authority from block 0 (amend #3).  new-in-txn is the
	 * minimal-WAL case whose initial bulk-load block references may not be in
	 * WAL at all, so no seed / WAL-scan can recover the true size; the authority
	 * instead owns every extend (including the in-txn bulk load), eliminating
	 * the seed.  The per-extend cross-node round trip is a real cost (perf
	 * forward spec-5.19a), accepted in exchange for closing the seed命门.
	 */
	return CLUSTER_HW_GLOBALIZE;
}

/*
 * cluster_hw_alloc_segment -- master-side batch allocator (authority math).
 *
 *	cur_hwm is the next free block number.  Grant a contiguous run starting at
 *	cur_hwm, truncating at the block-number ceiling (MaxBlockNumber) the same
 *	way cluster_sq_alloc_segment truncates at seqmax.  The HWM is strictly
 *	monotone, so two consecutive allocations never overlap.
 */
BlockNumber
cluster_hw_alloc_segment(BlockNumber cur_hwm, uint32 want, uint32 *granted, BlockNumber *new_hwm)
{
	uint64 avail;
	uint32 g;

	Assert(want > 0);
	Assert(granted != NULL && new_hwm != NULL);

	/*
	 * The valid block space is [0 .. MaxBlockNumber].  Once cur_hwm passes
	 * MaxBlockNumber (i.e. reaches InvalidBlockNumber) the relation is
	 * exhausted; grant nothing.
	 */
	if (cur_hwm > MaxBlockNumber) {
		*granted = 0;
		*new_hwm = cur_hwm;
		return InvalidBlockNumber;
	}

	avail = (uint64)MaxBlockNumber - (uint64)cur_hwm + 1;
	g = ((uint64)want <= avail) ? want : (uint32)avail;

	*granted = g;
	*new_hwm = cur_hwm + g;
	return cur_hwm;
}

/*
 * cluster_hw_serve_allowed -- the §3.1b R4/R6 serve gate (8.A, pure).
 *
 *	The HW authority may serve an HW_ALLOC for a (rel,fork) -- and in particular
 *	may auto-create an absent entry at block 0 -- ONLY when that resid's GRD
 *	shard is steady for its CURRENT remaster generation:
 *	  - phase == GRD_SHARD_NORMAL: no reconfig freeze / GES rebuild barrier is in
 *	    flight for the shard, and
 *	  - hw_rebuilt_generation == master_generation: the HW authority has rebuilt
 *	    this shard's HWMs from the dead master's snapshot + HW_RESERVE tail for
 *	    the generation the shard was last remastered to (§3.1b R4 step ③).
 *	When a survivor has just adopted a shard (P4 bumped master_generation but the
 *	HW rebuild + adoption snapshot have not completed, so hw_rebuilt_generation
 *	still lags), the per-(rel,fork) HWM is unknown; auto-creating at block 0 would
 *	re-hand an already-allocated range -> silent block reuse (R9).  Returns false
 *	there so the caller fails closed (53RA6), never an uncoordinated extend.
 *
 *	A never-remastered shard has master_generation == hw_rebuilt_generation == 0
 *	and phase NORMAL, so boot / steady state serves and a genuinely new relation's
 *	first-sight entry is safely created at 0 (the authority owns it from 0;
 *	recovery already replayed every prior HWM into the table before serving).
 */
bool
cluster_hw_serve_allowed(ClusterGrdShardPhase phase, uint32 master_generation,
						 uint32 hw_rebuilt_generation)
{
	return phase == GRD_SHARD_NORMAL && master_generation == hw_rebuilt_generation;
}
