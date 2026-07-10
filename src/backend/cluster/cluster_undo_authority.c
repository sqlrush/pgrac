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

/*
 * cluster_undo_route_decide -- pure serve routing.
 *
 *	OWNER_LIVE          -> serve from the (live) owner (D6 path).
 *	OK                  -> serve from the elected authority.
 *	RECOVERING/UNKNOWN  -> destination -1 (fail closed).  There is NO branch
 *	                       to a GRD hash-master: undo is owner-as-master, so a
 *	                       miss fails closed, never falls back to hash routing
 *	                       (spec-5.22d 约束 #2).
 */
ClusterUndoServeRoute
cluster_undo_route_decide(int32 owner_node, uint64 reconfig_epoch,
						  ClusterUndoAuthorityStatus status, int32 authority_node)
{
	ClusterUndoServeRoute r;

	r.reconfig_epoch = reconfig_epoch;
	r.status = status;

	switch (status) {
	case CLUSTER_UNDO_AUTHORITY_OWNER_LIVE:
		r.destination_node = owner_node;
		break;
	case CLUSTER_UNDO_AUTHORITY_OK:
		r.destination_node = authority_node;
		break;
	case CLUSTER_UNDO_AUTHORITY_RECOVERING:
	case CLUSTER_UNDO_AUTHORITY_UNKNOWN:
	default:
		r.destination_node = -1; /* fail closed; never a hash master */
		break;
	}
	return r;
}

/*
 * cluster_undo_authority_serve_decide -- pure consumer serve decision (D4-4).
 *
 *	See cluster_undo_authority.h for the contract.  The ONLY two provable
 *	actions are the live-owner path (D6, unchanged) and the self-authority
 *	block0 serve; every other route -- RECOVERING, UNKNOWN, a malformed
 *	destination, and an elected PEER authority (wire serve = D4-5/D4-6) --
 *	fails closed.
 */
ClusterUndoAuthorityServeDecision
cluster_undo_authority_serve_decide(const ClusterUndoServeRoute *route, int32 self_node)
{
	switch (route->status) {
	case CLUSTER_UNDO_AUTHORITY_OWNER_LIVE:
		return CLUSTER_UNDO_AUTHORITY_SERVE_OWNER_LIVE;
	case CLUSTER_UNDO_AUTHORITY_OK:
		if (self_node >= 0 && route->destination_node == self_node)
			return CLUSTER_UNDO_AUTHORITY_SERVE_SELF_BLOCK0;
		/* peer authority: wire serve lands with D4-5/D4-6; fail closed */
		return CLUSTER_UNDO_AUTHORITY_SERVE_FAIL_CLOSED;
	case CLUSTER_UNDO_AUTHORITY_RECOVERING:
	case CLUSTER_UNDO_AUTHORITY_UNKNOWN:
	default:
		return CLUSTER_UNDO_AUTHORITY_SERVE_FAIL_CLOSED;
	}
}

/*
 * cluster_undo_authority_coverage_ok -- pure coverage predicate (D4-4).
 *
 *	See cluster_undo_authority.h: three-way AND, each term independently
 *	required (约束 #3; U5 pins the shape).
 */
bool
cluster_undo_authority_coverage_ok(bool claimed_at_epoch, bool block0_readable, bool wrap_match)
{
	return claimed_at_epoch && block0_readable && wrap_match;
}

#endif /* USE_PGRAC_CLUSTER */
