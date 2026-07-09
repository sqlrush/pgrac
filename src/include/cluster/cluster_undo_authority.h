/*-------------------------------------------------------------------------
 *
 * cluster_undo_authority.h
 *	  Dead/absent-owner undo serve-authority derivation (spec-5.22d D4-1).
 *
 *	  When an undo resource's owner instance is dead/absent, the block0
 *	  TTSlot verdict for that owner cannot be served by the owner itself.
 *	  A single deterministic SURVIVOR is elected as the temporary serve
 *	  authority for that owner's undo resource, scoped to the current
 *	  reconfig (membership) epoch.  The authority reads the owner's block0
 *	  from shared storage under GCS and serves the verdict; every reader
 *	  agrees on WHO the authority is because the election is a pure
 *	  deterministic function of the accepted membership snapshot -- never
 *	  an ad-hoc local peer_state read (mirrors cluster_grd_master_map_
 *	  remaster's survivor rule).
 *
 *	  This header exposes the PURE decision core (cluster_undo_authority_
 *	  decide): it takes the membership snapshot as explicit bitmaps and
 *	  returns the elected authority + status, with no shmem / GRD / GUC
 *	  dependencies, so it is exhaustively unit-testable (spec-5.22d §4.1
 *	  U1-U8).  The live-snapshot wrapper (cluster_undo_serve_authority_
 *	  lookup) that reads the real GRD / membership / epoch state lives in
 *	  a separate object and is covered by the D4-8 TAP.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_undo_authority.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22d-undo-dead-owner-verdict-serve.md (D4-1, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_UNDO_AUTHORITY_H
#define CLUSTER_UNDO_AUTHORITY_H

#include "c.h"
#include "cluster/cluster_reconfig.h" /* CLUSTER_RECONFIG_DEAD_BITMAP_BYTES */

#ifdef USE_PGRAC_CLUSTER

/*
 * Outcome of a serve-authority derivation for one dead/absent owner.
 *
 *	OK          -- out_authority_node holds the elected survivor authority;
 *	               the caller routes the verdict request to it (or self-
 *	               serves if authority == self).
 *	OWNER_LIVE  -- the owner is a fresh-alive member; do NOT elect an
 *	               authority, the caller stays on the live-owner path (D6).
 *	RECOVERING  -- membership not yet settled at the requested epoch, or no
 *	               fresh survivor exists.  Transient; fail closed (retry).
 *	UNKNOWN     -- owner is neither fresh-alive nor dead-decided (生死未定),
 *	               or the input is malformed.  Fail closed; NEVER guess.
 *
 * RECOVERING / UNKNOWN both map to a fail-closed verdict (RESOURCE_RECOVERING
 * / 53R97) at the consumer -- the distinction is observability only.
 */
typedef enum ClusterUndoAuthorityStatus {
	CLUSTER_UNDO_AUTHORITY_OK = 0,
	CLUSTER_UNDO_AUTHORITY_OWNER_LIVE,
	CLUSTER_UNDO_AUTHORITY_RECOVERING,
	CLUSTER_UNDO_AUTHORITY_UNKNOWN
} ClusterUndoAuthorityStatus;

/*
 * Membership snapshot for one derivation, as explicit bitmaps (128-bit,
 * CLUSTER_RECONFIG_DEAD_BITMAP_BYTES = 16, bit n = byte[n>>3] & (1<<(n&7))).
 * The wrapper fills these from the accepted reconfig snapshot; the pure
 * decide() reads ONLY this struct.
 *
 *	request_epoch   -- the reconfig epoch the caller is scoped to.
 *	snapshot_epoch  -- the current accepted reconfig epoch.  Mismatch =>
 *	                   RECOVERING (the caller's epoch is stale; re-derive).
 *	declared        -- declared topology (pgrac.conf), ascending node ids.
 *	alive_fresh     -- declared members that are alive AND heartbeat-fresh
 *	                   (L420: cluster_reconfig_get_observed_fresh_alive),
 *	                   NOT mere record/slot existence.
 *	dead_decided    -- the accepted reconfig dead set (durable quorum
 *	                   decision), NOT a transient live-signal read (L419).
 */
typedef struct ClusterUndoAuthorityInput {
	int32 owner_node;
	uint64 request_epoch;
	uint64 snapshot_epoch;
	uint8 declared[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 alive_fresh[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
	uint8 dead_decided[CLUSTER_RECONFIG_DEAD_BITMAP_BYTES];
} ClusterUndoAuthorityInput;

typedef struct ClusterUndoAuthorityDecision {
	ClusterUndoAuthorityStatus status;
	int32 authority_node; /* valid iff status == OK, else -1 */
} ClusterUndoAuthorityDecision;

/*
 * Pure serve-authority election.  No shmem / GRD / GUC dependencies.
 *
 *	Rule (mirrors cluster_grd_master_map_remaster survivor selection):
 *	  authority = lowest node id that is (declared AND alive_fresh),
 *	  elected ONLY when the owner is dead-decided at the caller's epoch.
 *	Returns via *out; the return value echoes out->status for convenience.
 */
extern ClusterUndoAuthorityStatus cluster_undo_authority_decide(const ClusterUndoAuthorityInput *in,
																ClusterUndoAuthorityDecision *out);

/*
 * Live-snapshot wrapper (D4-1 second half, implemented in a heavy object,
 * TAP-covered).  Reads the real GRD / membership / freshness / epoch state,
 * fills a ClusterUndoAuthorityInput, and calls cluster_undo_authority_decide.
 *	owner_node     -- undo resource owner (canonical, from D1 resid).
 *	reconfig_epoch -- caller's scoped epoch (cluster_epoch_get_current()).
 *	*out_authority -- elected authority node when the return is OK, else -1.
 */
extern ClusterUndoAuthorityStatus
cluster_undo_serve_authority_lookup(int32 owner_node, uint64 reconfig_epoch, int32 *out_authority);

#endif /* USE_PGRAC_CLUSTER */

#endif /* CLUSTER_UNDO_AUTHORITY_H */
