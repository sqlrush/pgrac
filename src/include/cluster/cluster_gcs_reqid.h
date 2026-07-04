/*-------------------------------------------------------------------------
 *
 * cluster_gcs_reqid.h
 *	  GCS block request-id domain tagging (spec-6.14a D1).
 *
 *	  The master-side invalidate-broadcast slot identifies a round by
 *	  (request_id, epoch, tag) and rejects a late ACK only when its
 *	  request_id differs from the current slot's.  Request ids come from
 *	  several independent monotone counters -- one per requester backend
 *	  (ClusterGcsBlockBackendBlock.next_request_id, starts at 1) and one
 *	  per node for local-master S->X upgrades (local_upgrade_request_seq,
 *	  starts at 1).  Raw counter values therefore collide across sources
 *	  (every counter emits 1, 2, 3 ...), so two consecutive broadcasts for
 *	  the same tag in the same epoch can carry the SAME id from different
 *	  sources -- and a late ACK from the earlier round then falsely
 *	  certifies a holder in the newer round (ABA: the holder may have
 *	  re-acquired S in between; granting X over its live copy is a stale-S
 *	  hazard).
 *
 *	  Fix: partition the 64-bit id space into disjoint per-source domains,
 *	  so ids from different sources can never be equal and a late ACK can
 *	  never match a slot claimed by another source.  Within one source the
 *	  counter is monotone, so consecutive rounds from the same source
 *	  always differ.
 *
 *	    requester (backend) id:  [63]=0 | [62:56]=node | [55:40]=backend |
 *	                             [39:0]=seq   (seq of 0 maps to 1)
 *	    local-upgrade id:        [63]=1 | [62:56]=node | [55:0]=seq
 *	                             (seq of 0 maps to 1)
 *
 *	  The top BIT (not byte) is the local-upgrade domain flag, so the
 *	  scheme holds for node 0 as well (a plain node<<56 tag would leave
 *	  node0's ids untagged).  A request id is never 0: 0 is the broadcast
 *	  slot's idle sentinel.
 *
 *	  Pure computation over c.h types only -- usable from cluster_unit
 *	  SIMPLE tests without backend linkage.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_gcs_reqid.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14a-cf-s-revoke-x-grant.md (D1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_GCS_REQID_H
#define CLUSTER_GCS_REQID_H

#include "c.h"

#define GCS_REQID_LOCAL_DOMAIN_FLAG (UINT64CONST(1) << 63)
#define GCS_REQID_NODE_SHIFT 56
#define GCS_REQID_NODE_MASK UINT64CONST(0x7f)
#define GCS_REQID_BACKEND_SHIFT 40
#define GCS_REQID_BACKEND_MASK UINT64CONST(0xffff)
#define GCS_REQID_REQUESTER_SEQ_MASK ((UINT64CONST(1) << 40) - 1)
#define GCS_REQID_LOCAL_SEQ_MASK ((UINT64CONST(1) << 56) - 1)

/*
 * gcs_reqid_requester -- compose a requester-domain request id.
 *	node: cluster_node_id (negative single-node fallback masks to 0x7f,
 *	harmless -- no peers exist to collide with).  backend: MyBackendId-1
 *	ordinal.  seq: the per-backend monotone counter; its low 40 bits are
 *	used, a wrapped 0 maps to 1 (never equal to the idle sentinel).
 */
static inline uint64
gcs_reqid_requester(int node, int backend_ord, uint64 seq)
{
	uint64 low = seq & GCS_REQID_REQUESTER_SEQ_MASK;

	if (low == 0)
		low = 1;
	return (((uint64)node & GCS_REQID_NODE_MASK) << GCS_REQID_NODE_SHIFT)
		   | (((uint64)backend_ord & GCS_REQID_BACKEND_MASK) << GCS_REQID_BACKEND_SHIFT) | low;
}

/*
 * gcs_reqid_local_upgrade -- compose a local-upgrade-domain request id.
 *	Top bit set: disjoint from every requester domain, including node 0's.
 */
static inline uint64
gcs_reqid_local_upgrade(int node, uint64 seq)
{
	uint64 low = seq & GCS_REQID_LOCAL_SEQ_MASK;

	if (low == 0)
		low = 1;
	return GCS_REQID_LOCAL_DOMAIN_FLAG
		   | (((uint64)node & GCS_REQID_NODE_MASK) << GCS_REQID_NODE_SHIFT) | low;
}

#endif /* CLUSTER_GCS_REQID_H */
