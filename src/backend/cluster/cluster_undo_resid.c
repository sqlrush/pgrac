/*-------------------------------------------------------------------------
 *
 * cluster_undo_resid.c
 *	  Shared-undo block resource identity + owner-as-master routing --
 *	  pure layer (spec-5.22a D1).
 *
 *	  This file ships the backend-pure layer: the undo resid
 *	  encoder/decoder, the class discriminator, the owner-as-master
 *	  routing function, and the anti-ABA generation predicate.  None of
 *	  these touch elog / shmem / locks, so the cluster_unit test links the
 *	  object standalone.  The data plane that consumes this identity
 *	  (grant / PI / block serving / recovery materialization / retention)
 *	  lands with later deliverables.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_resid.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.22a-undo-block-resource-identity.md (D1, §2.2 / §3.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_scn.h" /* SCN_NODE_ID_VALID */
#include "cluster/cluster_undo_resid.h"

/*
 * cluster_undo_resid_encode -- build the undo-block resource id.
 *
 *	field3 is the segment reuse generation (the segment header wrap_count),
 *	so a recycled segment lands a distinct resource (ABA defence),
 *	mirroring the HW relfilenode field.  field4 is the owning instance:
 *	the undo authority lives at the owner, so the owner is part of the
 *	identity, not a routing afterthought.
 */
void
cluster_undo_resid_encode(int32 owner_node, uint32 undo_segment, uint32 block_no, uint32 generation,
						  ClusterResId *dst)
{
	Assert(dst != NULL);
	if (dst == NULL)
		return;
	Assert(SCN_NODE_ID_VALID(owner_node));

	dst->field1 = undo_segment;
	dst->field2 = block_no;
	dst->field3 = generation;
	dst->field4 = (uint16)owner_node;
	dst->type = CLUSTER_UNDO_RESID_TYPE;
	dst->lockmethodid = DEFAULT_LOCKMETHOD;
}

/*
 * cluster_undo_resid_decode -- split an undo resid back into its fields.
 *
 *	The caller must pass an undo-class resid (Assert enforced); the
 *	decoder is the wire-ABI boundary, so it never guesses at foreign
 *	classes.
 */
void
cluster_undo_resid_decode(const ClusterResId *rid, int32 *owner_node, uint32 *undo_segment,
						  uint32 *block_no, uint32 *generation)
{
	Assert(rid != NULL);
	if (rid == NULL || owner_node == NULL || undo_segment == NULL || block_no == NULL
		|| generation == NULL)
		return;
	Assert(rid->type == CLUSTER_UNDO_RESID_TYPE);

	*owner_node = (int32)rid->field4;
	*undo_segment = rid->field1;
	*block_no = rid->field2;
	*generation = rid->field3;
}

/*
 * cluster_undo_resid_is_undo -- class discriminator.
 */
bool
cluster_undo_resid_is_undo(const ClusterResId *rid)
{
	Assert(rid != NULL);
	if (rid == NULL)
		return false;

	return rid->type == CLUSTER_UNDO_RESID_TYPE;
}

/*
 * cluster_undo_resid_master -- owner-as-master routing.
 *
 *	Returns the encoded owner_node directly: the undo authority lives at
 *	the owning instance, so the master is part of the identity and is
 *	NEVER derived from a shard hash.  A hash-derived master would place
 *	the authority at a node that does not own the undo, which is exactly
 *	the misrouting the GRD-side guard fails closed on.
 */
int32
cluster_undo_resid_master(const ClusterResId *rid)
{
	Assert(rid != NULL);
	if (rid == NULL)
		return -1;
	Assert(rid->type == CLUSTER_UNDO_RESID_TYPE);

	return (int32)rid->field4;
}

/*
 * cluster_undo_resid_generation_matches -- anti-ABA check.
 *
 *	false means the reference predates a whole-segment recycle (the
 *	segment header wrap_count moved on) and the caller MUST fail closed;
 *	it must never be treated as a match.
 */
bool
cluster_undo_resid_generation_matches(const ClusterResId *rid, uint32 expected_generation)
{
	Assert(rid != NULL);
	if (rid == NULL)
		return false;
	Assert(rid->type == CLUSTER_UNDO_RESID_TYPE);

	return rid->field3 == expected_generation;
}
