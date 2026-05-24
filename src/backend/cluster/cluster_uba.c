/*-------------------------------------------------------------------------
 *
 * cluster_uba.c
 *	  pgrac UBA (Undo Block Address) helper out-of-line implementations
 *	  (spec-3.4b D1).
 *
 *	  Most UBA helpers (uba_encode / uba_decode / uba_get_segment_id /
 *	  uba_get_tt_slot_offset) are inline in cluster_uba.h.  This file
 *	  provides the two-tier helper uba_origin_node_id, which references
 *	  CLUSTER_UNDO_SEGS_PER_INSTANCE owned by cluster_undo_alloc.h, plus
 *	  any future non-inline helpers.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.4b-real-tt-allocator-uba-encoding-production-cross-node.md
 *       (v0.3 FROZEN 2026-05-24)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_uba.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_uba.h"
#include "cluster/storage/cluster_undo_alloc.h"	/* CLUSTER_UNDO_SEGS_PER_INSTANCE */


/*
 * uba_origin_node_id
 *
 *	Pure O(1) derivation from segment_id using the per-instance segment-id
 *	range encoding documented in cluster_undo_alloc.h:
 *
 *	    segment_id = (owner_instance - 1) * CLUSTER_UNDO_SEGS_PER_INSTANCE
 *	                + per_instance_slot + 1
 *	    owner_instance = ((segment_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE) + 1
 *	    node_id        = owner_instance - 1
 *
 *	Returns InvalidNodeId when the UBA is invalid or the derived node_id
 *	falls outside [0, SCN_MAX_VALID_NODE_ID].
 *
 *	This intentionally does NOT read the segment header on disk; the
 *	closed-form derivation keeps the visibility reader hot path
 *	allocation-free and safe under buffer content lock.
 */
NodeId
uba_origin_node_id(UBA u)
{
	uint32 seg_id;
	uint32 blk_no;
	uint16 tt_off;
	uint16 row_off;
	uint32 derived;

	if (!uba_decode(u, &seg_id, &blk_no, &tt_off, &row_off))
		return InvalidNodeId;

	derived = (seg_id - 1) / CLUSTER_UNDO_SEGS_PER_INSTANCE;
	if (derived > (uint32) SCN_MAX_VALID_NODE_ID)
		return InvalidNodeId;

	return (NodeId) derived;
}
