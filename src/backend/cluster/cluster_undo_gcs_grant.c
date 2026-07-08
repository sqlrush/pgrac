/*-------------------------------------------------------------------------
 *
 * cluster_undo_gcs_grant.c
 *	  Shared-undo block GCS integration -- reader S-grant runtime primitive
 *	  (spec-5.22b D2-3).
 *
 *	  This is the runtime wrapper that composes the pure decision core
 *	  (cluster_undo_gcs.c: cluster_undo_grant_armed / _admissible) with the
 *	  live wire: owner-as-master routing (D2-1), the reused 6.12i undo-TT
 *	  fetch (owner ships its own image; the peer never opens the foreign undo
 *	  file, invariant #8), and the fail-closed admission gate (Rule 8.A).
 *
 *	  It lives in its own object because it references the heavy backend
 *	  surface (the GCS block fetch, the membership epoch, the coherence GUC)
 *	  that the pure routing/decision object (cluster_undo_gcs.o) deliberately
 *	  does NOT, so cluster_unit links that object standalone.  This runtime
 *	  wrapper is forward-covered by the D2-7 / D6 TAP legs (a peer really
 *	  reading a foreign owner's undo block end-to-end), not by cluster_unit.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_gcs_grant.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-5.22b-undo-block-gcs-integration.md (D2, §2.2/§3.1/§3.2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_cr_server.h" /* cluster_gcs_block_undo_tt_fetch_and_wait */
#include "cluster/cluster_epoch.h"	   /* cluster_epoch_get_current */
#include "cluster/cluster_gcs_block.h" /* GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER */
#include "cluster/cluster_guc.h"	   /* cluster_undo_gcs_coherence */
#include "cluster/cluster_mode.h"	   /* cluster_peer_mode_enabled, cluster_node_id */
#include "cluster/cluster_undo_gcs.h"
#include "cluster/cluster_undo_resid.h"

/*
 * cluster_undo_block_acquire_shared -- reader S-grant primitive (D2-3).
 *
 *	See cluster_undo_gcs.h for the full contract.  A peer acquires a coherent
 *	S view of owner N's undo block through owner-as-master routing; any
 *	miss / DENIED / doubt returns false so the caller keeps its 53R97
 *	fail-closed boundary (Rule 8.A -- MVCC/visibility never forward-links a
 *	false-visible edge).
 */
bool
cluster_undo_block_acquire_shared(const ClusterResId *undo_resid, uint32 expected_generation,
								  char *dst_block, ClusterUndoGrantResult *res)
{
	int32 owner;
	uint32 seg;
	uint32 block;
	uint32 gen;
	uint64 local_epoch;
	ClusterLiveAuthority auth;

	if (undo_resid == NULL || dst_block == NULL || res == NULL)
		return false; /* fail-closed */
	if (!cluster_undo_resid_is_undo(undo_resid))
		return false;

	memset(res, 0, sizeof(*res));

	/*
	 * Coherence gate.  At the default (coherence off, or a single-node / non
	 * peer-mode deployment) the path is not armed: the caller keeps its
	 * unchanged authority-less fetch path, so D2 lands inert (回归安全).
	 */
	if (!cluster_undo_grant_armed(cluster_undo_gcs_coherence, cluster_peer_mode_enabled()))
		return false;

	cluster_undo_resid_decode(undo_resid, &owner, &seg, &block, &gen);
	local_epoch = cluster_epoch_get_current();

	if (cluster_undo_block_master_is_self(undo_resid)) {
		/*
		 * master==self: the owner reading its OWN undo needs no network grant
		 * (local PCM suffices, §3.1).  The owner-incarnation epoch self-check
		 * (L364) that must gate serving from this local fast path -- proving
		 * THIS node is still the legitimate owner incarnation at local_epoch,
		 * not a fenced zombie -- has no standalone primitive yet, and this API
		 * has no D2-3 consumer (the owner reads its own undo through the
		 * existing local undo paths).  Rather than serve a local read that
		 * cannot yet be proven safe (Rule 8.A), the self fast path is
		 * fail-closed here and lands with its incarnation check + consumer in
		 * D6.  The peer-reads-foreign S-grant below is D2-3's delivered heart.
		 */
		return false; /* honest fail-closed forward (D6); never a stale local serve */
	}

	/*
	 * master!=self (live owner): the owner is the authority AND the holder.
	 * Reuse the 6.12i owner-ships-image wire verbatim (zero new wire; Q3★A
	 * maps a success to GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER at this layer).
	 * The owner reads its own undo and ships the current image + the authority
	 * co-sampled with it; this peer consumes the shipped image and NEVER opens
	 * the foreign undo file (invariant #8).  A DENIED_RESOURCE_RECOVERING
	 * (owner dead / undo shard remastering) or any timeout / checksum /
	 * missing-trailer collapses the fetch to false -> fail-closed 53R97
	 * (dead-owner serve is D4, not D2).
	 */
	if (!cluster_gcs_block_undo_tt_fetch_and_wait(owner, seg, block, dst_block, &auth))
		return false;

	/*
	 * Admit only a coherent view: segment-generation anti-ABA AND the owner's
	 * incarnation epoch equals ours AND the shipped durable high-water is
	 * present (Rule 8.A -- never admit on doubt).  anchor_lsn is Invalid at
	 * the block level; the version-coverage (hwm >= page_lsn) gate is applied
	 * by the D3/D6 verdict consumer that owns the tuple's anchor.
	 */
	if (!cluster_undo_grant_admissible(undo_resid, expected_generation, auth, local_epoch,
									   InvalidXLogRecPtr))
		return false;

	res->origin_epoch = auth.origin_epoch;
	res->live_hwm_lsn = auth.live_hwm_lsn;
	res->tt_generation = auth.tt_generation;
	res->status = (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
	return true;
}
