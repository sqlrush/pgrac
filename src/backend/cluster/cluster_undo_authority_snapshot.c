/*-------------------------------------------------------------------------
 *
 * cluster_undo_authority_snapshot.c
 *	  Live membership-snapshot wrapper for dead/absent-owner undo
 *	  serve-authority election (spec-5.22d D4-1, wrapper half).
 *
 *	  Reads the real membership-state SSOT (cluster_membership_get_state),
 *	  heartbeat freshness (cluster_reconfig_get_observed_fresh_alive, the
 *	  L420 freshness gate -- NOT record existence), and the current accepted
 *	  reconfig epoch (cluster_epoch_get_current), projects them into a
 *	  ClusterUndoAuthorityInput, and defers the actual election to the pure
 *	  cluster_undo_authority_decide().  Kept out of cluster_undo_authority.c
 *	  so the pure decision object links standalone in the D4-1 unit test;
 *	  this wrapper is covered by the D4-8 TAP.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_authority_snapshot.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22d-undo-dead-owner-verdict-serve.md (D4-1, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"				/* CLUSTER_MAX_NODES */
#include "cluster/cluster_cr.h"					/* undo_authority_* counters (D4-5) */
#include "cluster/cluster_epoch.h"				/* cluster_epoch_get_current */
#include "cluster/cluster_membership.h"			/* cluster_membership_get_state */
#include "cluster/cluster_reconfig.h"			/* cluster_reconfig_get_observed_fresh_alive */
#include "cluster/cluster_runtime_visibility.h" /* cluster_vis_tt_block_positive_proof */
#include "cluster/cluster_scn.h"				/* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_tt_slot.h"			/* TT_WRAP_INVALID */
#include "cluster/cluster_undo_authority.h"
#include "cluster/cluster_undo_resid.h" /* cluster_undo_resid_master (D1) */
#include "cluster/cluster_undo_smgr.h"	/* cluster_undo_smgr_read_block (D4-5) */

#ifdef USE_PGRAC_CLUSTER

static inline void
bm_set(uint8 *bm, int32 node)
{
	bm[node >> 3] |= (uint8)(1u << (node & 7));
}

/*
 * cluster_undo_serve_authority_lookup -- live-snapshot wrapper.
 *
 *	Projects the membership SSOT into the pure decide()'s bitmaps:
 *	  alive_fresh  <- state==MEMBER AND observed_fresh_alive (L420)
 *	  dead_decided <- state==DEAD or state==REMOVED (durable fail-stop /
 *	                  decommission decision; NOT a transient live read, L419)
 *	  declared     <- every non-absent, non-rejected known node
 *	ABSENT / JOINING / REJECTED owners are neither fresh nor dead-decided,
 *	so decide() fails closed (UNKNOWN) -- we never guess a node dead.
 */
ClusterUndoAuthorityStatus
cluster_undo_serve_authority_lookup(int32 owner_node, uint64 reconfig_epoch, int32 *out_authority)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;
	int32 node;

	*out_authority = -1;

	memset(&in, 0, sizeof(in));
	in.owner_node = owner_node;
	in.request_epoch = reconfig_epoch;
	in.snapshot_epoch = cluster_epoch_get_current();

	for (node = 0; node <= SCN_MAX_VALID_NODE_ID && node < CLUSTER_MAX_NODES; node++) {
		ClusterMembershipState st = cluster_membership_get_state(node);

		switch (st) {
		case CLUSTER_MEMBER_MEMBER:
			bm_set(in.declared, node);
			if (cluster_reconfig_get_observed_fresh_alive(node))
				bm_set(in.alive_fresh, node);
			break;
		case CLUSTER_MEMBER_DEAD:
		case CLUSTER_MEMBER_REMOVED:
			bm_set(in.declared, node);
			bm_set(in.dead_decided, node);
			break;
		case CLUSTER_MEMBER_JOINING:
			/* known/declared but not eligible either way (may become live) */
			bm_set(in.declared, node);
			break;
		case CLUSTER_MEMBER_ABSENT:
		case CLUSTER_MEMBER_REJECTED:
		default:
			/* not a usable authority; owner here => decide() UNKNOWN */
			break;
		}
	}

	(void)cluster_undo_authority_decide(&in, &out);
	if (out.status == CLUSTER_UNDO_AUTHORITY_OK)
		*out_authority = out.authority_node;
	return out.status;
}

/*
 * cluster_undo_serve_authority -- resolve the serve route for one undo
 * resource (D4-2 heavy glue).
 *
 *	Canonical owner comes from the D1 pure identity (cluster_undo_resid_
 *	master, never a hash route); the live serve authority comes from the
 *	D4-1 wrapper; the pure D4-2 mapper turns the two into a route.  A lookup
 *	miss yields a fail-closed route (destination -1), NEVER the GRD hash
 *	master (spec-5.22d 约束 #2 / §2.2).
 */
ClusterUndoServeRoute
cluster_undo_serve_authority(const ClusterResId *undo_resid, uint64 reconfig_epoch)
{
	int32 owner_node = cluster_undo_resid_master(undo_resid);
	int32 authority_node = -1;
	ClusterUndoAuthorityStatus st;

	st = cluster_undo_serve_authority_lookup(owner_node, reconfig_epoch, &authority_node);
	return cluster_undo_route_decide(owner_node, reconfig_epoch, st, authority_node);
}

/*
 * cluster_undo_authority_block0_prove -- shared authority block0 prove core
 * (D4-5).  See cluster_undo_authority.h for the contract.
 *
 *	The coverage three-way AND (§2.4), with the counters owned HERE so the
 *	requester self-serve leg (D4-4) and the wire-served LMS leg (D4-5/D4-6)
 *	cannot drift on the observability contract:
 *	  (i)   claimed_at_epoch: claim_epoch still IS the current accepted
 *	        epoch; a stale claim bumps epoch_stale_reject + fail_closed.
 *	  (ii)  block0_readable: the AUTHORITY_BLOCK0 foreign-owner intent
 *	        (D4-3) resolved + fully read the shared block0 bytes.
 *	  (iii) wrap_match: the slot-level xid+wrap positive proof matched
 *	        (content-based anti-ABA; the same proof the live CP3 leg
 *	        trusts).  block0 is synchronously durable (D2 Q3): a full read
 *	        is crash-consistent, no LSN-cover window applies.
 *	Proof NONE has no CP5 fallback: the owner is dead, there is nobody to
 *	ask, so NONE proves nothing => fail closed (never a native fallback).
 */
ClusterUndoVerdictResult
cluster_undo_authority_block0_prove(int32 owner_node, uint32 segment_id, TransactionId raw_xid,
									uint64 claim_epoch)
{
	ClusterUndoVerdictResult unknown
		= { .kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, .commit_scn = InvalidScn, .wrap = 0 };
	PGAlignedBlock page;
	SCN scn = InvalidScn;
	uint16 wrap = TT_WRAP_INVALID;
	ClusterVisTtProof proof = CLUSTER_VIS_TT_PROOF_NONE;
	bool claimed;
	bool readable = false;

	/* malformed owner: never provable (defense in depth; the route layers
	 * upstream already refuse it) */
	if (owner_node < 0 || owner_node > SCN_MAX_VALID_NODE_ID || !TransactionIdIsNormal(raw_xid)) {
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	/* segment 0 is bootstrap-only (Assert-fenced in uba_encode): a ref
	 * carrying it is not resolvable evidence */
	if (segment_id == 0 || segment_id > UINT16_MAX) {
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	/* (i) the claim must still be scoped to the CURRENT epoch */
	claimed = (cluster_epoch_get_current() == claim_epoch);
	if (!claimed)
		cluster_undo_authority_note_epoch_stale_reject();

	/* (ii) read the dead owner's shared block0 (read-only foreign intent) */
	if (claimed)
		readable = cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0,
												segment_id, (uint8)(owner_node + 1), 0 /* block0 */,
												page.data);

	/* (iii) slot-level xid+wrap positive proof on the read bytes */
	if (readable)
		proof = cluster_vis_tt_block_positive_proof(page.data, segment_id, (uint8)(owner_node + 1),
													raw_xid, &scn, &wrap);

	if (!cluster_undo_authority_coverage_ok(claimed, readable,
											proof != CLUSTER_VIS_TT_PROOF_NONE)) {
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	cluster_undo_authority_note_serve_hit();
	return cluster_undo_verdict_from_block_proof(proof, scn, wrap);
}

#endif /* USE_PGRAC_CLUSTER */
