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
#include "cluster/cluster_cssd.h"	   /* cluster_cssd_get_peer_state (D2-5 serve-gate) */
#include "cluster/cluster_epoch.h"	   /* cluster_epoch_get_current */
#include "cluster/cluster_gcs_block.h" /* GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER */
#include "cluster/cluster_grd.h"	   /* cluster_grd_recovery_in_progress (D2-5 serve-gate) */
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
 *
 *	spec-5.22e D5-8 contract: every caller must pass the read-admission
 *	gate (cluster_undo_horizon_read_admission_enforce) BEFORE reaching this
 *	primitive -- the sole current caller (try_resolve_remote) gates at its
 *	entry.  A new caller that skips admission reopens the pre-join /
 *	mixed-capability consumption hole.
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
		cluster_undo_gcs_count_local_fast_path(); /* D2-6: master==self routing taken */
		return false; /* honest fail-closed forward (D6); never a stale local serve */
	}

	/*
	 * D2-5 remaster serve-gate (§3.4).  Under owner-as-master the undo shard's
	 * master IS its owner, so the owner-death / remaster window IS the undo
	 * shard's recovery phase.  Deny BEFORE the fetch when the owner is not a
	 * live serving master -- a converged-DEAD owner OR an in-flight remaster
	 * episode -- so the peer fail-closes as DENIED_RESOURCE_RECOVERING instead
	 * of stalling on a doomed fetch to a dead node (dead-owner SERVE from shared
	 * storage is D4, not D2; Rule 8.A never false-resolves).  The owner-liveness
	 * read mirrors cluster_gcs_block_phase_for_tag's CSSD-DEAD +
	 * recovery-in-progress fence, keyed on the OWNER (owner-as-master) rather
	 * than a hash static master; undo folds into the reconfig episode by reading
	 * its state, never by joining the hash-shard master map (D1-5).
	 */
	if (!cluster_undo_serve_allowed(cluster_cssd_get_peer_state(owner) != CLUSTER_CSSD_PEER_DEAD,
									cluster_grd_recovery_in_progress())) {
		res->status = (uint8)GCS_BLOCK_REPLY_DENIED_RESOURCE_RECOVERING;
		cluster_undo_gcs_count_remaster_deny(); /* D2-6 */
		return false;							/* fail-closed; serve = D4 */
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
	cluster_undo_gcs_count_grant_shared(GCS_BLOCK_DATA_SIZE); /* D2-6: S-grant + shipped bytes */
	return true;
}

/*
 * cluster_undo_block_acquire_exclusive -- writer/cleaner X-grant primitive
 * (D2-4).
 *
 *	See cluster_undo_gcs.h for the full contract.  The owner takes X on its OWN
 *	undo block before mutating/recycling it; a peer must never take X on a
 *	foreign owner's undo (single-writer invariant), so master != self fails
 *	closed (Rule 8.A).  Runtime object; its live consumer + the D6
 *	owner-incarnation self-check are skeleton-ahead (Q-D24-3).
 */
bool
cluster_undo_block_acquire_exclusive(const ClusterResId *undo_resid)
{
	if (undo_resid == NULL)
		return false; /* fail-closed */
	if (!cluster_undo_resid_is_undo(undo_resid))
		return false;

	/*
	 * Coherence gate.  At the default (coherence off / non peer-mode) the owner
	 * keeps its unchanged local undo write path, so D2 lands inert (回归安全).
	 */
	if (!cluster_undo_grant_armed(cluster_undo_gcs_coherence, cluster_peer_mode_enabled()))
		return false;

	/*
	 * Single-writer invariant: only the owner ever writes/recycles its own undo
	 * (cluster_undo_record.c owner_instance == self).  A peer taking X on a
	 * foreign owner's undo would be writing foreign undo -> fail closed
	 * (Rule 8.A), never grant.  master == self => local PCM X, zero network (the
	 * owner is already both master and the sole writer).  The owner-incarnation
	 * self-check (L364) that must gate actually serving from this local fast
	 * path lands with the write-path consumer in D6.
	 */
	if (!cluster_undo_block_master_is_self(undo_resid))
		return false;

	cluster_undo_gcs_count_grant_exclusive(); /* D2-6: local X grant */
	cluster_undo_gcs_count_local_fast_path(); /* D2-6: master==self, zero network */
	return true;
}

/*
 * cluster_undo_block_invalidate_peers -- owner-write PI-discard broadcast
 * (D2-4).
 *
 *	See cluster_undo_gcs.h for the full contract.  After the owner durably
 *	writes its undo block's current copy, direct every peer holding an obsolete
 *	Past Image to drop it.  SCN-only (Q-D24-2); LMON-context (L172); returns the
 *	number of peers notified, all misses fail-safe (0).  Skeleton-ahead until
 *	D6 registers undo PI holders (Q-D24-3).
 */
int
cluster_undo_block_invalidate_peers(const ClusterResId *undo_resid, SCN write_scn)
{
	int32 owner;
	uint32 seg;
	uint32 block;
	uint32 gen;
	BufferTag tag;
	uint32 holders = 0;
	int notified = 0;
	int n;

	if (undo_resid == NULL)
		return 0;
	if (!cluster_undo_resid_is_undo(undo_resid))
		return 0;

	/* Not armed (single-node / coherence off) -> no peers to invalidate. */
	if (!cluster_undo_grant_armed(cluster_undo_gcs_coherence, cluster_peer_mode_enabled()))
		return 0;

	cluster_undo_resid_decode(undo_resid, &owner, &seg, &block, &gen);
	(void)owner; /* synthetic undo tag deliberately omits owner (the origin only
				  * ever serves its own undo; fail-closed by construction) */
	(void)gen;	 /* segment generation is the READER's anti-ABA dimension
				  * (cluster_undo_grant_admissible), not the write-side notify */

	/*
	 * The undo block's synthetic address tag (spec-6.12i D-i1): undo segments
	 * live outside shared buffers, so the PI protocol keys them by this magic
	 * tag exactly as the fetch wire does.
	 */
	tag = GcsBlockUndoFetchTagMake(seg, block);

	/*
	 * SCN coverage judge under the entry lock: if the just-durable write reaches
	 * the block's SCN watermark, collect + clear the pi_holders_bitmap (both
	 * watermarks) and hand back the pre-clear set to notify.  Missing entry /
	 * not-covered / unarmed watermark -> false (fail-safe: the PI merely lingers
	 * until buffer pressure / anti-ABA reread, never a correctness loss).  Until
	 * D6 registers undo PI holders this fail-safes to 0 (Q-D24-3).
	 */
	if (!cluster_pcm_lock_pi_discard_collect(tag, write_scn, &holders))
		return 0;

	/*
	 * Direct a PI_DISCARD to each PEER whose bit is set (the owner is the
	 * current writer, never a PI holder of its own block -- notify_target
	 * excludes self).  Reuses the shipped single-target INVALIDATE send
	 * (LMON-context; L172 -- the D6 live caller runs in the owner-as-master
	 * LMON dispatch/tick path).
	 */
	for (n = 0; n < 32; n++) {
		if (!cluster_undo_pi_discard_notify_target(holders, n, cluster_node_id))
			continue;
		cluster_gcs_block_send_pi_discard_invalidate(tag, n);
		notified++;
	}

	cluster_undo_gcs_count_invalidate_notify(notified); /* D2-6 */
	return notified;
}
