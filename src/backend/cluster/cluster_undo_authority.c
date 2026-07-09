/*-------------------------------------------------------------------------
 *
 * cluster_undo_authority.c
 *	  Dead/absent-owner undo serve-authority election (spec-5.22d D4-1).
 *
 *	  See cluster_undo_authority.h for the model.  This file holds the PURE
 *	  decision core; the live-snapshot wrapper (reading real GRD / membership
 *	  / epoch state) lands in D4-1's second half.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_authority.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22d-undo-dead-owner-verdict-serve.md (D4-1, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_scn.h" /* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_undo_authority.h"

#ifdef USE_PGRAC_CLUSTER

/* Test bit n in a 128-bit little-endian membership bitmap. */
static inline bool
au_bit_test(const uint8 *bm, int32 node)
{
	return (bm[node >> 3] >> (node & 7)) & 1u;
}

/*
 * cluster_undo_authority_decide -- pure serve-authority election.
 *
 *	Precedence (all fail-closed except OK / OWNER_LIVE):
 *	  1. malformed owner (outside [0, SCN_MAX_VALID_NODE_ID])   -> UNKNOWN
 *	  2. caller epoch != accepted snapshot epoch                -> RECOVERING
 *	  3. owner is fresh-alive                                   -> OWNER_LIVE
 *	  4. owner not dead-decided (生死未定; never guess)          -> UNKNOWN
 *	  5. elect lowest (declared AND fresh-alive) survivor;
 *	     none exists                                            -> RECOVERING
 *	     else                                                   -> OK
 *
 *	Mirrors cluster_grd_master_map_remaster: survivors are the declared
 *	list minus non-fresh nodes; the election is a deterministic function
 *	of the accepted snapshot so every reader agrees on the authority.
 */
ClusterUndoAuthorityStatus
cluster_undo_authority_decide(const ClusterUndoAuthorityInput *in,
							  ClusterUndoAuthorityDecision *out)
{
	int32 node;

	out->authority_node = -1;

	/* 1. malformed owner */
	if (in->owner_node < 0 || in->owner_node > SCN_MAX_VALID_NODE_ID)
		return (out->status = CLUSTER_UNDO_AUTHORITY_UNKNOWN);

	/* 2. caller scoped to a stale epoch */
	if (in->snapshot_epoch != in->request_epoch)
		return (out->status = CLUSTER_UNDO_AUTHORITY_RECOVERING);

	/* 3. owner still fresh-alive: stay on the live-owner (D6) path */
	if (au_bit_test(in->alive_fresh, in->owner_node))
		return (out->status = CLUSTER_UNDO_AUTHORITY_OWNER_LIVE);

	/* 4. owner neither fresh-alive nor dead-decided: 生死未定, never guess */
	if (!au_bit_test(in->dead_decided, in->owner_node))
		return (out->status = CLUSTER_UNDO_AUTHORITY_UNKNOWN);

	/* 5. elect the lowest declared, fresh-alive survivor */
	for (node = 0; node <= SCN_MAX_VALID_NODE_ID; node++) {
		if (au_bit_test(in->declared, node) && au_bit_test(in->alive_fresh, node)) {
			out->authority_node = node;
			return (out->status = CLUSTER_UNDO_AUTHORITY_OK);
		}
	}

	/* no fresh survivor -> transient; fail closed */
	return (out->status = CLUSTER_UNDO_AUTHORITY_RECOVERING);
}

#endif /* USE_PGRAC_CLUSTER */
